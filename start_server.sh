#!/bin/sh
slicer serve slicer.ini 2> slicer.log &
echo $! > slicer.pid