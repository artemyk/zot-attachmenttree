from tendo import singleton
me = singleton.SingleInstance()

import os, tempfile, shutil, sys, time
import numpy as np
import pandas as pd
from collections import defaultdict
import logging
logger = logging.getLogger()
sep = os.path.sep

# Function to convert string into a safe filename
import unicodedata, re, string
validFilenameChars = "-_.() %s%s" % (string.ascii_letters, string.digits)
validRE = re.compile("[^%s]" % validFilenameChars)
def scrubfilename(filename):
  cleanedFilename = unicodedata.normalize('NFKD', unicode(filename)).encode('ASCII', 'ignore')
  return validRE.sub(' ', cleanedFilename).strip()

# Function to get dataframe with item names
def get_itemnames_df(db):
  sql = """
  select items.itemID,
    creatorData.firstName as authorfirst, 
    creatorData.lastName as authorlast, 
    SUBSTR(dateDV.value,1,4) as publishedDate,
    titleDV.value as title,
    pubDV.value as journal
    
  from items
    left join (
         select itemID, min(orderIndex) as minOrderIndex from itemCreators group by itemID
      ) AS firstCreatorIndex ON firstCreatorIndex.itemID = items.itemID 
    left join itemCreators on itemCreators.itemID=items.itemID and itemCreators.orderIndex=firstCreatorIndex.minOrderIndex
    left join creators     on creators.creatorID=itemCreators.creatorID 
    left join creatorData  on creatorData.creatorDataID=creators.creatorDataID      
    
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
      itemNamesDF.authorfirst.apply(lambda x: "".join(map(lambda y: y[0] if len(y) else "", x.split(" "))) + " " if x is not None else "") + \
      itemNamesDF.authorlast.apply(lambda x: x.strip() if x is not None else "")
  itemNamesDF['fname'] = itemNamesDF.fname.apply(lambda x: x + ' - ' if x != "" else '')
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.title.apply(lambda x: x[0:70] if x is not None and x != "" else "NOTITLE")
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.journal.apply(lambda x: " - " + x if x is not None else "")
  itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.publishedDate.apply(lambda x: " - " + x if x is not None else "")
  return itemNamesDF

def get_profile_dir():
  import getpass
  import platform
  home = os.path.expanduser("~")
  if platform.system() == 'Darwin':
    basepaths = [home+u'/Library/Application Support/Firefox/',home+u'/Library/Application Support/Zotero/']
  elif platform.system() == 'Windows':
    raise Exception("Windows not supported due to symlinks")
    #if map(int, platform.version().split("."))[0] >= 6:
    #    basepaths = [home+u'\\AppData\\Roaming\\Mozilla\\Firefox\\', 
    #                 home+u'\\AppData\\Roaming\\Zotero\\Zotero\\']
    #else:
    #    user = getpass.getuser()
    #    basepaths = [u'C:\\Documents and Settings\\%s\\Application Data\\Mozilla\\Firefox\\'%user,
    #                 u'C:\\Documents and Settings\\%s\\Application Data\\Zotero\\'%user]
  else:
    basepaths = [home+'/.mozilla/firefox/',home+'/.zotero/']

  if os.path.exists(basepaths[0]) and os.path.exists(basepaths[1]):
    raise Exception('Both standalone and firefox version exists -- not sure which to choose')

  profiledir = None    
  for bp in basepaths:
    cdir = bp + 'Profiles' + sep
    if os.path.exists(cdir):
      for f2 in (f for f in os.listdir(cdir) if not f.startswith('.')):
        if profiledir is None:
          profiledir = cdir + f2 + os.path.sep + 'zotero' + os.path.sep
        else:
          raise Exception('Duplicate profiles found')

  return profiledir


# ************************************************
# Define and read in command-line arguments
# ************************************************
import argparse
def commandline_arg(bytestring):  # Parse unicode
  unicode_string = bytestring.decode(sys.getfilesystemencoding())
  return unicode_string
parser = argparse.ArgumentParser(description='Update directory tree of Zotero attachments.')
parser.add_argument('dest', type=commandline_arg, help='Output location')
parser.add_argument('--db', type=commandline_arg, metavar='FILE', nargs='?', help='Location of zotero.sqlite file (automatically determined if not specified)')
parser.add_argument('--latency', metavar='L', help='Polling interval in seconds', type=int, default=10)
parser.add_argument('--debug', help='Output debugging information', action='store_true')
parser.add_argument('--test', help='Don\'t modify file system, only do simulated test run',  action='store_true')
parser.add_argument('--nodaemon', help='Run once and exit.', action='store_true')
args = parser.parse_args()

if args.debug or args.test:
  logger.setLevel(logging.DEBUG)

# ************************************************
# Find Zotero database file
# ************************************************
if args.db is None:
  try:
    profiledir = get_profile_dir()
    dbfile = profiledir + 'zotero.sqlite'
    logging.debug(u'Zotero DB found at: %s', dbfile)
  except:
    logging.exception("Error finding Zotero profile directory")
    sys.exit()
else:
  dbfile = args.db
  profiledir = os.path.dirname(dbfile) + sep
  logging.debug(u"Zotero DB specified at %s", dbfile)

try:
  tmppath = tempfile.mkdtemp()
except:
  logging.exception("Error making temporary directory")
  sys.exit()

tempdb = tmppath + 'zotero.sqlite'

# ************************************************
# Determine output directory
# ************************************************
OUTPUTDIR = os.path.expanduser(args.dest)
logging.debug("Saving to destination folder: %s", OUTPUTDIR)

last_modtime = 0

# ************************************************
# Begin polling loop
# ************************************************
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
    logging.debug("Running startup sync")
  else:
    logging.debug("Database modification detected")

  start_time = time.time()
  last_modtime = cur_modtime

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
    def get_collection_tree(df, basefold, parentCollectionId = np.nan):
        global foldlist, existing_folder_names
        if not np.isnan( parentCollectionId ): 
          foldlist.append((parentCollectionId,basefold))
        try:
            cdf = df.ix[parentCollectionId]
            for cid, cname in zip(cdf.collectionID, cdf.collectionName):
                ndx = 1
                basedir = sep.join(basefold[:]) + sep
                while True:
                  cfoldname = scrubfilename(cname) + ("" if ndx == 1 else " (%d)" % ndx)
                  if basedir + cfoldname not in existing_folder_names:
                    existing_folder_names.add(basedir + cfoldname)
                    break
                  ndx +=1

                cfold = basefold[:] + [cfoldname,]
                get_collection_tree(df, cfold, cid)
        except:
            #no children
            pass
    df = pd.read_sql( "select parentCollectionID, collectionID, collectionName from collections", db, index_col="parentCollectionID")
    get_collection_tree(df, [])


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

    dfatt = pd.read_sql("""
                        select sourceItemID, itemAttachments.itemID, path 
                        from itemAttachments 
                        left join deletedItems deleted on itemAttachments.itemID = deleted.itemID
                        left join deletedItems deleted2 on itemAttachments.sourceItemID = deleted2.itemID
                        where 
                          deleted.itemID IS NULL AND 
                          mimeType='application/pdf' and 
                          path like 'storage:%' and
                          sourceItemId is not null
                        order by itemAttachments.itemID
                        """, 
                        db)
    itemAtts = defaultdict(list)
    attpath = {}
    for sId, iId, cPath in zip(dfatt.sourceItemID, dfatt.itemID, dfatt.path):
      itemAtts[int(sId)].append(int(iId))
      attpath[iId] = cPath[8:]
        
    dfhashkey = pd.read_sql("select itemID, key from items", db, index_col="itemID").key.to_dict()

    trg_structure = [(OUTPUTDIR,'DIR'),]
    def addsymlinks(foldname, itemslist):
      global trg_structure
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
            sname = sep.join(foldname + [fname + (' (%d)' % (ndx+1) if l > 1 else '') + '.pdf',])
            lnktarget = profiledir + 'storage' + sep + dfhashkey[cItemID] + sep + attpath[cItemID]
            trg_structure.append((OUTPUTDIR+sname, 'LINK', lnktarget))
            
    for collId, foldname in foldlist:
      try:
        items = df.loc[collId]
        itemlist = [items.itemID,] if len(items) == 1 else items.itemID.tolist()
      except:  # Folder without documents
        itemlist = []
      addsymlinks(foldname, itemlist)

    addsymlinks(['Unfiled',], itemNamesDF[itemNamesDF.incollection==False].index.values)

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
            existing_structure.append((fullpath,'LINK', readlink(fullpath)))
          else:
            existing_structure.append((fullpath,'FILE'))

      for f in os.listdir(root):
        if not f.startswith('.') and os.path.isdir(root+sep+f) and os.path.islink(root+sep+f):
          existing_structure.append((root+sep+f,'DIRLINK'))

    trg_structure_set = set(trg_structure)
    existing_structure_set = set(existing_structure)
    
    def objname(o):
      if o[1] == 'LINK':
        return 'LINK ' + o[0] + " (" + o[2] + ")"
      else:
        return o[1] + ' ' + o[0]

    for f in existing_structure[::-1]: 
      # iterate backward so that deeper down directories are deleted first
      if f not in trg_structure_set:
        logging.debug(u"Deleting %s", objname(f))
        if not args.test:
          if f[1] == 'DIR':
            shutil.rmtree(f[0])
          else:
            os.remove(f[0])

    for f in trg_structure:
      if f not in existing_structure_set:
        logging.debug(u"Making %s", objname(f))
        if not args.test:
          try:
            if f[1] == 'DIR':
              os.makedirs(f[0])
            elif f[1] == 'FILE':
              with open(f[0], "w") as fhandle:
                  fhandle.write("")
            elif f[1] == 'LINK':
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

  logging.debug("Executed in %0.3fs", time.time() - start_time)

  if args.nodaemon:
    break

logging.debug("Removing temporary directory")
try:
  shutil.rmtree(tmppath)
except:
  pass
