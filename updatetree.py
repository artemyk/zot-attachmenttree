import os, tempfile, shutil, sys, time
import numpy as np
import pandas as pd
from collections import defaultdict
import logging
logger = logging.getLogger()
sep = os.path.sep


import unicodedata, re, string
validFilenameChars = "-_.() %s%s" % (string.ascii_letters, string.digits)
validRE = re.compile("[^%s]" % validFilenameChars)
def scrubfilename(filename):
  cleanedFilename = unicodedata.normalize('NFKD', unicode(filename)).encode('ASCII', 'ignore')
  return validRE.sub(' ', cleanedFilename).strip()


import argparse
def commandline_arg(bytestring):
  unicode_string = bytestring.decode(sys.getfilesystemencoding())
  return unicode_string
parser = argparse.ArgumentParser(description='Update tree of Zotero attachments.')
parser.add_argument('dest', type=commandline_arg, help='Ouptut location')
parser.add_argument('--db', type=commandline_arg, metavar='SQLITE_FILE', nargs='?', help='Location of zotero.sqlite file (automatically determined if not specified)')
parser.add_argument('--latency', help='Polling interval in seconds', type=int, default=10)
parser.add_argument('--debug', help='Output debugging information', action='store_true')
parser.add_argument('--test', help='Don\'t modify file system, only do simulated test run',  action='store_true')
args = parser.parse_args()

if args.debug or args.test:
  logger.setLevel(logging.DEBUG)

if args.db is None:
  try:
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
    dbfile = profiledir + 'zotero.sqlite'
    logging.debug('Zotero DB found at: %s' % dbfile)
  except:
    logging.exception("Error finding Zotero profile directory")
    sys.exit()
else:
  dbfile = args.db
  logger.exception("Zotero DB specified at %s" % dbfile)

try:
  tmppath = tempfile.mkdtemp()
except:
  logging.exception("Error making temporary directory")
  sys.exit()

tempdb = tmppath + 'zotero.sqlite'


OUTPUTDIR = os.path.expanduser(args.dest)
logger.debug("Saving to destination folder: %s", OUTPUTDIR)

last_modtime = 0

while True:

  msg = None
  if last_modtime != 0:  # dont sleep the first time around
      time.sleep(args.latency)
      msg = 'Database modification detected'

  cur_modtime = os.stat(dbfile).st_mtime
  if cur_modtime == last_modtime:
    continue

  if msg is not None:
    logger.debug(msg)

  last_modtime = cur_modtime

  shutil.copyfile(dbfile, tempdb)

  try:
    import sqlite3
    db = sqlite3.connect(tempdb)
  except:
    logging.exception("Error opening Zotero database")
    shutil.rmtree(tmppath)
    sys.exit()

  try:
    # Initial item names
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
      
    where items.itemTypeID != 1 and items.itemTypeID != 14 -- don't want attachments or notes
    """
    itemNamesDF=pd.read_sql(sql, db, index_col='itemID')

    itemNamesDF['fname'] = \
        itemNamesDF.authorfirst.apply(lambda x: "".join(map(lambda y: y[0] if len(y) else "", x.split(" "))) + " " if x is not None else "") + \
        itemNamesDF.authorlast.apply(lambda x: x.strip() if x is not None else "")
    itemNamesDF['fname'] = itemNamesDF.fname.apply(lambda x: x + ' - ' if x != "" else '')
    itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.title.apply(lambda x: x[0:70] if x is not None and x != "" else "NOTITLE")
    itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.journal.apply(lambda x: " - " + x if x is not None else "")
    itemNamesDF['fname'] = itemNamesDF['fname'] + itemNamesDF.publishedDate.apply(lambda x: " - " + x if x is not None else "")
    itemNamesDF['incollection'] = False


    # Get folders
    foldlist = []
    def get_collection_tree(df, basefold, parentCollectionId = np.nan):
        global foldlist
        if not np.isnan( parentCollectionId ): foldlist.append((parentCollectionId,basefold))
        try:
            cdf = df.ix[parentCollectionId]
            for cid, cname in zip(cdf.collectionID, cdf.collectionName):
                cfold = basefold[:] + [scrubfilename(cname),]
                get_collection_tree(df, cfold, cid)
        except:
            #no children
            pass
    df = pd.read_sql( "select parentCollectionID, collectionID, collectionName from collections", db, index_col="parentCollectionID")
    get_collection_tree(df, [])


    # Create list of symlinks
    namedict = itemNamesDF.fname.to_dict()

    df = pd.read_sql("""
                    select collectionItems.itemID, collectionID 
                     from collectionItems
                     INNER JOIN items ON collectionItems.itemID = items.itemID
                     WHERE items.itemTypeID != 1 AND items.itemTypeID != 14
                     """, db, index_col='collectionID')
    df['itemID'] = df['itemID'].astype('int')
    itemNamesDF.loc[df.itemID, 'incollection'] = True

    dfatt = pd.read_sql("""
                        select sourceItemID, itemID, path from itemAttachments 
                        where mimeType='application/pdf' and path like 'storage:%'
                        and sourceItemId is not null
                        order by itemID
                        """, 
                        db)
    itemAtts = defaultdict(list)
    attpath = {}
    for sId, iId, cPath in zip(dfatt.sourceItemID, dfatt.itemID, dfatt.path):
      itemAtts[int(sId)].append(int(iId))
      attpath[iId] = cPath[8:]
        
    dfhashkey = pd.read_sql("select itemID, key from items", db, index_col="itemID").key.to_dict()

    symlinks = []

    def addsymlinks(foldname, itemslist):
      global symlinks
      for sourceItemID in itemslist:
        fname = scrubfilename(namedict[sourceItemID])
        attlist = itemAtts[sourceItemID]
        l = len(attlist)
        if l == 0:
          symlinks.append((sep.join(foldname + [fname + ' NOPDF',]), None))
        else:
          for ndx, cItemID in enumerate(attlist):
            cfname = fname + (' (%d)' % (ndx+1) if l > 1 else '') + '.pdf'
            symlinks.append((sep.join(foldname + [cfname,]), 
                             profiledir + 'storage' + sep + dfhashkey[cItemID] + sep + attpath[cItemID]))
            
    for collId, foldname in foldlist:
      try:
        items = df.loc[collId]
      except:  # Folder without documents
        continue
      addsymlinks(foldname, [items.itemID,] if len(items) == 1 else items.itemID.tolist())

    addsymlinks(['Unfiled',], itemNamesDF[itemNamesDF.incollection==False].index.values)

    # ****************************
    # Update destination directory
    # ****************************

    alldirs, allfiles, alldirlinks = {}, {}, []
    for root, dirs, files in os.walk(OUTPUTDIR):
      alldirs[root] = True
      for f in files:
        allfiles[root+sep+f] = True
      for f in os.listdir(root):
        if not f.startswith('.') and os.path.isdir(root+sep+f) and os.path.islink(root+sep+f):
          alldirlinks.append(root+sep+f)

    for l in alldirlinks:
      logger.debug("%s: Removing symbolic link to directory", l)
      if not args.test: os.remove(l)

    for _, foldname in foldlist + [(None,['Unfiled',]),]:
      cdir = OUTPUTDIR + sep.join(foldname)
      if cdir in alldirs:
        alldirs[cdir]=False # do not delete
      else:
        logger.debug("%s: Creating directories" % cdir)
        if not args.test: os.makedirs(cdir)

    if OUTPUTDIR in alldirs:
      del alldirs[OUTPUTDIR]

    for lnk, src in symlinks:
      clnk = OUTPUTDIR + lnk
      if clnk in allfiles:
        if (src is None and not os.path.islink(clnk)) or (src is not None and os.readlink(clnk) == src):
          allfiles[clnk] = False # don't delete

    logdone=False
    for f, to_del in allfiles.iteritems():
      if to_del:
        if not logdone:
          logger.debug("***DELETING FILES***")
          logdone = True
        logger.debug(f)
        if not args.test: os.remove(f)
            
    logdone=False
    for d, to_del in alldirs.iteritems():
      if to_del:
        if not logdone:
          logger.debug("***DELETING DIRS***")
          logdone = True
        logger.debug(d)
        if not args.test: shutil.rmtree(d)
            
    logdone=False
    for lnk, src in symlinks:
      clnk = OUTPUTDIR + lnk
      if allfiles.get(clnk, True):
        if not logdone:
          logger.debug("***INSERTING LINKS***")
          logdone = True
        logger.debug("%s -> %s", clnk, str(src))
        if os.path.lexists(clnk):
          logger.debug("  (already exists, skipping!)")
          continue
        if src is None:
          if not args.test:
            with open(clnk, "w") as f:
                f.write("")
        else:
          if not args.test:
            os.symlink(src, clnk)
    db.close()
    os.remove(tempdb)

  except:
    logging.exception("Exception occured")
    try:
      db.close()
    except:
      pass
    sys.exit()


logger.debug("Removing temporary directory")
try:
  shutil.rmtree(tmppath)
except:
  pass
