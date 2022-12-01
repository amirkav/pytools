#!/usr/bin/env bash
set -e

mkdir /out/bin
mkdir /out/lib
mkdir /out/python
cp /root/bin/antiword /out/bin
cp -r /root/.antiword /out/lib/antiword
cp -r /var/lang/lib/python3.6/site-packages/cffi* /out/python
cp -r /var/lang/lib/python3.6/site-packages/cryptography* /out/python
cp -r /var/lang/lib/python3.6/site-packages/pdfminer* /out/python
