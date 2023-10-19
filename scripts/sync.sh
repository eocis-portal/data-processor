#!/bin/bash

rootfolder=`dirname $0`/..

rsync -avr $rootfolder/src dev@eocis.org:/home/dev/services/data-processor
rsync -av $rootfolder/setup.cfg dev@eocis.org:/home/dev/services/data-processor
rsync -av $rootfolder/pyproject.toml dev@eocis.org:/home/dev/services/data-processor


