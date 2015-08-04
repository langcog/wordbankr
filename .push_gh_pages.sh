#!/bin/bash

rm -rf out || exit 0;
mkdir out;

GH_REPO="@github.com/langcog/wordbankr.git"
FULL_REPO="https://$GH_TOKEN$GH_REPO"

for files in '*.tar.gz'; do
        tar xfz $files
done

cd out
git init
git config user.name "wordbankr-travis"
git config user.email "travis"
cp ../wordbankr/inst/doc/wordbankr.html index.html

git add .
git commit -m "deployed to github pages"
git push --force --quiet $FULL_REPO master:gh-pages
