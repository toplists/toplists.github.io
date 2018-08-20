#!/bin/bash

cd "$(dirname "$0")"
eval `ssh-agent -s`
cd toplists.github.io
git pull
cd ..

./website_corr.py
./website_daytoday.py

cd toplists.github.io
git commit -am "automated update"
git push


