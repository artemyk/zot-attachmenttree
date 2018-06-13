# -*- coding: utf-8 -*-
from __future__ import print_function
import os, tempfile, shutil, sys, time, datetime
import psutil
import numpy as np
import pandas as pd
from collections import defaultdict
import logging
logger = logging.getLogger()
sep = os.path.sep

import unicodedata
def scrubfilename(filename):
  # Function to convert string into a safe filename
  # Remove diacritics and leave only allowed characters 
  # Supports unicode
  keepcharacters = "-_.() '"
  s = u""
  for c in unicodedata.normalize('NFD', filename):
    if unicodedata.category(c) == 'Mn':
      continue
    if c.isalnum() or c in keepcharacters:
      s += c
    elif c == u'’':
      s += "'"
    else:
      s += " "
  return s.rstrip()

def is_zotero_version5(db):
    version = pd.read_sql( "select version from version where schema='userdata'", db).version[0]
    
    return version >= 90

# Function to get dataframe with item names
def get_itemnames_df(db):
  if is_zotero_version5(db):
    joins = "left join creators as cData on cData.creatorID=itemCreators.creatorID"
  else:
    joins = """
    left join creators on creators.creatorID=itemCreators.creatorID 
    left join creatorData as cData on creatorData.creatorDataID=creators.creatorDataID
    """
  sql = """
  select items.itemID,
    cData.firstName as authorfirst, 
    cData.lastName as authorlast, 
    SUBSTR(dateDV.value,1,4) as publishedDate,
    titleDV.value as title,
    pubDV.value as journal
    
  from items
    left join (
         select itemID, min(orderIndex) as minOrderIndex from itemCreators group by itemID
      ) AS firstCreatorIndex ON firstCreatorIndex.itemID = items.itemID 
    left join itemCreators on itemCreators.itemID=items.itemID and itemCreators.orderIndex=firstCreatorIndex.minOrderIndex
    """+joins+"""
    left join itemData dateD         on (dateD.itemID=items.itemID and dateD.fieldID=14) 
    left join itemDataValues dateDV  on dateDV.valueID=dateD.valueID
    left join itemData titleD         on (titleD.itemID=items.itemID and titleD.fieldID=110) 
    left join itemDataValues titleDV  on titleDV.valueID=titleD.valueID
    left join itemData pubD         on (pubD.itemID=items.itemID and pubD.fieldID=12) 
    left join itemDataValues pubDV  on pubDV.valueID=pubD.valueID
    left join deletedItems deleted on items.itemID = deleted.itemID
    
  where 
  deleted.itemID IS NULL AND 
  items.itemTypeID != 1 and items.itemTypeID != 14 -- don't want attachments or notes
  """

  itemNamesDF=pd.read_sql(sql, db, index_col='itemID')

  itemNamesDF['fname'] = \
      itemNamesDF.authorlast.apply(lambda x: x.strip() if x is not None else "NOLAST") + " " + \
      itemNamesDF.authorfirst.apply(lambda x: "".join(map(lambda y: y[0] if len(y) else "", x.split(" "))) if x is not None else "")
  itemNamesDF['fname'] = itemNamesDF.fname.apply(lambda x: x + ' - ' if x != "" else '')
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.title.apply(lambda x: x[0:70] if x is not None and x != "" else "NOTITLE")
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.journal.apply(lambda x: " - " + x if x is not None else "")
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.publishedDate.apply(lambda x: " - " + x if x is not None else "")
  return itemNamesDF

def is_zotero_running(is_standalone):
  for pid in psutil.pids():
      p = psutil.Process(pid)
      try:
        pname = p.name()
        if pname.endswith('.exe'):
          pname = pname[:-4]

        if is_standalone and pname == "zotero":
          return True

        elif not is_standalone and pname == "firefox":
          return True

      except:
        pass

  return False

def get_profile_dir(only_standalone=False, only_browser=False):
  import platform
  home = os.path.expanduser("~")

  def expdir(cdir):
    if cdir[-1] != sep:
      cdir += sep
    if not os.path.exists(cdir):
      return []
    else:
      return [cdir + f for f in os.listdir(cdir) if not f.startswith('.')]

  if platform.system() == 'Darwin':
    browserpaths    = expdir(home+u'/Library/Application Support/Firefox/Profiles/')
    standalonepaths = expdir(home+u'/Library/Application Support/Zotero/Profiles/') + [home+u'/Zotero/']
  elif platform.system() == 'Windows':
    raise Exception("Windows not supported due to symlinks")
    #if map(int, platform.version().split("."))[0] >= 6:
    #    browserpaths = expdir(home+u'\\AppData\\Roaming\\Mozilla\\Firefox\\')
    #    standalonepaths = expdir(home+u'\\AppData\\Roaming\\Zotero\\Zotero\\')
    #else:
    #    import getpass
    #    user = getpass.getuser()
    #    browserpaths = expdir(u'C:\\Documents and Settings\\%s\\Application Data\\Mozilla\\Firefox\\Profiles\\'%user)
    #    standalonepaths = expdir(u'C:\\Documents and Settings\\%s\\Application Data\\Zotero\\Profiles\\'%user)
  else:
    browserpaths = expdir(home+'/.mozilla/firefox/Profiles/')
    standalonepaths = expdir(home+'/.zotero/Profiles/')

  if only_standalone:
    searchpaths = standalonepaths
  elif only_browser:
    searchpaths = browserpaths
  else:
    searchpaths = standalonepaths + browserpaths
    if any( os.path.exists(p) for p in browserpaths ) and \
       any( os.path.exists(p) for p in standalonepaths ):
      raise Exception('Both Firefox (%s) and standalone (%s) version of Zotero exists -- not sure which to choose' % tuple(basepaths))

  profiledir = None 
  is_standalone = None  
  for bp in searchpaths:
    if os.path.exists(bp) and os.path.isfile(bp + 'zotero.sqlite'):
      if profiledir is None:
        profiledir = bp
        if bp in standalonepaths:
          is_standalone = True
        elif bp in browserpaths:
          is_standalone = False
        else:
          raise Exception('Path %s not in standalonepaths or browserpaths' % bp)

      else:
        raise Exception('Duplicate profiles found')

  return profiledir, is_standalone


# ************************************************
# Define and read in command-line arguments
# ************************************************
import argparse
def commandline_arg(bytestring):  # Parse unicode
  if sys.version_info >= (3, 0):
    unicode_string = bytestring
  else:
    unicode_string = bytestring.decode(sys.getfilesystemencoding())
  return unicode_string
parser = argparse.ArgumentParser(description='Update directory tree of Zotero attachments.')
parser.add_argument('dest', type=commandline_arg, help='Output location')
parser.add_argument('--db', type=commandline_arg, metavar='FILE', nargs='?', 
  help='Location of zotero.sqlite file (try to automatically determine if not specified)')
parser.add_argument('--standalone', action='store_true', help='Use zotero.sqlite from standalone Zotero')
parser.add_argument('--browser', action='store_true', help='Use zotero.sqlite from Firefox-plugin Zotero')
parser.add_argument('--latency', metavar='L', help='Polling interval in seconds', type=int, default=10)
parser.add_argument('--verbose', help='Verbosity level of debugging information', type=int, default=0, choices=[0,1])
parser.add_argument('--test', help='Don\'t modify file system, only do simulated test run',  action='store_true')
parser.add_argument('--nodaemon', help='Run once and exit.', action='store_true')
args = parser.parse_args()

from tendo import singleton
me = singleton.SingleInstance()

if args.verbose == 1 or args.test:
  logger.setLevel(logging.INFO)
#if args.verbose == 2:
#  logger.setLevel(logging.DEBUG)
if args.test:
  logging.info('Running in test mode (no modifications will be made)')


# ************************************************
# Find Zotero database file
# ************************************************
if args.db is None:
  if args.standalone and args.browser:
    logging.exception('Both --browser and --standalone options should not be specified')
  try:
    profiledir, is_standalone = get_profile_dir(args.standalone, args.browser)
    dbfile = profiledir + 'zotero.sqlite'
    logging.info(u'Zotero DB found at: %s', dbfile)
  except:
    logging.exception("Error finding Zotero profile directory")
    sys.exit()
else:
  dbfile = args.db
  profiledir = os.path.dirname(dbfile) + sep
  is_standalone = 'firefox' in profiledir.lower()
  logging.info(u"Zotero DB specified at %s", dbfile)

try:
  tmppath = tempfile.mkdtemp()
except:
  logging.exception("Error making temporary directory")
  sys.exit()

# ************************************************
# Determine output directory
# ************************************************
OUTPUTDIR = os.path.expanduser(args.dest)
if OUTPUTDIR[-1] != sep:
  OUTPUTDIR += sep

logging.info("Saving to destination folder: %s", OUTPUTDIR)

last_modtime = 0

# ************************************************
# Begin polling loop
# ************************************************
ROOT_COLLECTION_ID = -1000
tempdb = tmppath + 'zotero.sqlite'

while True:

  if last_modtime != 0:  # dont sleep the first time around
      time.sleep(args.latency)

  try:
    cur_modtime = os.stat(dbfile).st_mtime
  except:
    raise Exception("Could not find database file %s" % dbfile)
  if cur_modtime == last_modtime:  # Has database been modified?
    continue

  if last_modtime == 0:
    logging.info("Running startup sync")
  else:
    ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    logging.info("Database modification detected (%s)" % ctime)

  start_time = time.time()
  last_modtime = cur_modtime

  if not is_zotero_running(is_standalone):
    logging.error("Zotero not running!")
    continue

  # ************************************************
  # Create copy of database and connect to it
  # ************************************************
  shutil.copyfile(dbfile, tempdb)
  try:
    import sqlite3
    db = sqlite3.connect(tempdb)
  except:
    logging.exception("Error opening Zotero database")
    shutil.rmtree(tmppath)
    sys.exit()

  try:
    # Pull out Zotero item names 
    itemNamesDF = get_itemnames_df(db)
    itemNamesDF['incollection'] = False

    # ******************************************************
    # Use collections database to create directory structure
    # ******************************************************


    foldlist = []
    existing_folder_names = set()
    def get_collection_tree(df, basefold, parentCollectionId):
        global foldlist, existing_folder_names
        if parentCollectionId != ROOT_COLLECTION_ID:
            foldlist.append((parentCollectionId,basefold))
        try:
            cdf = df.loc[[parentCollectionId]]
            for cid, cname in zip(cdf.collectionID, cdf.collectionName):
                ndx = 1
                basedir = sep.join(basefold[:]) + sep
                while True:
                  cfoldname = "+" + scrubfilename(cname) + ("" if ndx == 1 else " (%d)" % ndx)
                  if basedir + cfoldname not in existing_folder_names:
                    existing_folder_names.add(basedir + cfoldname)
                    break
                  ndx +=1

                cfold = basefold[:] + [cfoldname,]
                get_collection_tree(df, cfold, cid)
        except KeyError:
            #no children
            pass
            
    df = pd.read_sql("""
        select ifnull(parentCollectionID, %d) as parentCollectionID, 
          collectionID, 
          collectionName 
          from collections""" % ROOT_COLLECTION_ID, db, index_col="parentCollectionID")
    get_collection_tree(df, [], ROOT_COLLECTION_ID)


    # ******************************************************
    # Create list of symbolic links to attachments
    # ******************************************************
    namedict = itemNamesDF.fname.to_dict()

    df = pd.read_sql("""
                     select collectionItems.itemID, collectionID 
                     from collectionItems
                     INNER JOIN items ON collectionItems.itemID = items.itemID
                     left join deletedItems deleted on items.itemID = deleted.itemID
                     where deleted.itemID IS NULL AND items.itemTypeID != 1 AND items.itemTypeID != 14
                     """, db, index_col='collectionID')
    df['itemID'] = df['itemID'].astype('int')
    itemNamesDF.loc[df.itemID, 'incollection'] = True

    if is_zotero_version5(db):
      # Zotero 5.0
      parentItemIdCol = 'parentItemId'
      mimeTypeCol     = 'contentType'
    else:
      parentItemIdCol = 'sourceItemId'
      mimeTypeCol     = 'mimeType'

    dfatt = pd.read_sql("""
                        select """+parentItemIdCol+""" as parentItemId, itemAttachments.itemID, path 
                        from itemAttachments 
                        left join deletedItems deleted on itemAttachments.itemID = deleted.itemID
                        left join deletedItems deleted2 on itemAttachments."""+parentItemIdCol+""" = deleted2.itemID
                        where 
                          deleted.itemID IS NULL AND 
                          """+mimeTypeCol+"""='application/pdf' and 
                          path like 'storage:%' and
                          """+parentItemIdCol+""" is not null
                        order by itemAttachments.itemID
                        """, 
                        db)
    itemAtts = defaultdict(list)
    attpath = {}
    for sId, iId, cPath in zip(dfatt.parentItemId, dfatt.itemID, dfatt.path):
      itemAtts[int(sId)].append(int(iId))
      attpath[iId] = cPath[8:]
        
    dfhashkey = pd.read_sql("select itemID, key from items", db, index_col="itemID").key.to_dict()

    trg_structure = [(OUTPUTDIR,'DIR'),]
    trg_dirs_lower = set()
    def addsymlinks(foldname, itemslist):
      global trg_structure, trg_dirs_lower
      trg_structure.append((OUTPUTDIR+sep.join(foldname), "DIR"))
      for sourceItemID in itemslist:
        fname = scrubfilename(namedict[sourceItemID])
        attlist = itemAtts[sourceItemID]
        l = len(attlist)
        if l == 0:
          sname = sep.join(foldname + [fname + ' NOPDF',])
          trg_structure.append((OUTPUTDIR+sname, 'FILE'))
        else:
          for ndx, cItemID in enumerate(attlist):
            #sname = sep.join(foldname + [fname + (' (%d)' % (ndx+1) if l > 1 else '') + '.pdf',])
            #lnktarget = profiledir + 'storage' + sep + dfhashkey[cItemID] + sep + attpath[cItemID]
            lnktarget = profiledir + 'storage' + sep + dfhashkey[cItemID]
            sfxnum = 1
            while True:
              sname = sep.join(foldname + [fname + (' (%d)' % sfxnum if sfxnum > 1 else ''),])
              if sname.lower() not in trg_dirs_lower:
                break
              sfxnum += 1

            if not os.path.exists(lnktarget):
              logging.warning("ERROR: Link to non-existent directory: %s -> %s" % (sname, lnktarget) )

            else:
              trg_structure.append((OUTPUTDIR+sname, 'DIRLINK', lnktarget))
              trg_dirs_lower.add(sname.lower())
            
    for collId, foldname in foldlist:
      try:
        items = df.loc[collId]
        itemlist = [items.itemID,] if len(items) == 1 else items.itemID.tolist()
      except:  # Folder without documents
        itemlist = []
      addsymlinks(foldname, itemlist)

    addsymlinks(['+Unfiled',], itemNamesDF[itemNamesDF.incollection==False].index.values)

    # ****************************
    # Update destination directory
    # ****************************
    islink = os.path.islink
    readlink = os.readlink
    existing_structure = []
    for root, dirs, files in os.walk(OUTPUTDIR):
      existing_structure.append((root,'DIR'))
      for f in files:
        if not f.startswith('.'):
          fullpath = root + sep + f
          if islink(fullpath):
            existing_structure.append((fullpath, 'LINK', readlink(fullpath)))
          else:
            existing_structure.append((fullpath,'FILE'))

      for f in os.listdir(root):
        fullpath = root + sep + f
        if not f.startswith('.') and os.path.isdir(fullpath) and os.path.islink(fullpath):
          existing_structure.append((root+sep+f,'DIRLINK', readlink(fullpath)))

    trg_structure_set = set(trg_structure)
    existing_structure_set = set(existing_structure)

    def objname(o):
      if o[1] == 'LINK' or o[1] == 'DIRLINK':
        return o[1] + ' ' + o[0] + " (" + o[2] + ")"
      else:
        return o[1] + ' ' + o[0]

    for f in existing_structure[::-1]: 
      # iterate backward so that deeper down directories are deleted first
      if f not in trg_structure_set:
        logging.info(u"Deleting %s", objname(f))
        if not args.test:
          if f[1] == 'DIR':
            shutil.rmtree(f[0])
          else:
            os.remove(f[0])

    for f in trg_structure:
      if f not in existing_structure_set:
        logging.info(u"Making %s", objname(f))
        if not args.test:
          try:
            if f[1] == 'DIR':
              os.makedirs(f[0])
            elif f[1] == 'FILE':
              with open(f[0], "w") as fhandle:
                  fhandle.write("")
            elif f[1] == 'LINK' or f[1] == 'DIRLINK':
              os.symlink(f[2], f[0])
            else:
              raise Exception(u"Don''t know how to create this type!")
          except:
            logging.exception("EXCEPTION: Creation failed")

    # ******************************************************
    # Close database and delete temporary file
    # ******************************************************
    db.close()
    os.remove(tempdb)

  except:
    logging.exception(u"EXCEPTION!")
    try:
      db.close()
    except:
      pass
    try:
      os.remove(tempdb)
    except:
      pass

  logging.info("Executed in %0.3fs", time.time() - start_time)

  if args.nodaemon:
    break

logging.info("Removing temporary directory")
try:
  shutil.rmtree(tmppath)
except:
  pass
