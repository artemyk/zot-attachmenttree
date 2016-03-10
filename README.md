# zot-attachmenttree

This is a Python script to create a hierarchical directory tree containing Zotero PDF attachments.
Each directory corresponds to a Zotero collection (items in more than one collection are duplicated).  
The files in the directories are symbolic links to attachments stored in the Zotero storage directory.

This directory can then be put on a cloud storage provider and synchronized to a tablet device, using apps like [Documents by Readle](https://readdle.com/products/documents) for the Ipad or [AutoSync Dropbox](https://play.google.com/store/apps/details?id=com.ttxapps.dropsync&hl=en) for Android.   Because files are symlinks to original PDFs, changes in the Dropbox directory (e.g. highlighting or adding notes on your tablet) will propagate back to the attachments in Zotero.

Not tested on Unix platforms.  Windows not currently supported due to symlinks, pull requests welcome.


# Command-line usage

Example usage:
```
python updatetree.py --debug ~/Dropbox/ZoteroPDFs/
```
will create a directory structure with your Zotero PDFs in `~/Dropbox/ZoteroPDFs/`.


More generally, the following options are available:

```
$ python updatetree.py --help
usage: updatetree.py [-h] [--db [FILE]] [--standalone] [--browser]
                     [--latency L] [--debug] [--test] [--nodaemon]
                     dest

Update directory tree of Zotero attachments.

positional arguments:
  dest          Output location

optional arguments:
  -h, --help    show this help message and exit
  --db [FILE]   Location of zotero.sqlite file (automatically determined if
                not specified)
  --standalone  Use zotero.sqlite from standalone Zotero
  --browser     Use zotero.sqlite from Firefox-plugin Zotero
  --latency L   Polling interval in seconds
  --debug       Output debugging information
  --test        Don't modify file system, only do simulated test run
  --nodaemon    Run once and exit.
```

# Requirements

The following Python packages should be installed:

* pandas
* numpy
* tendo
