Simple test of our Git Key-Value store
  $ mkdir simple
  $ cd simple
  $ git init -q 2> /dev/null
  $ git config init.defaultBranch main
  $ git checkout -b main -q
  $ git config user.email "romain@mirage.io"
  $ git config user.name "Romain Calascibetta"
  $ echo "Hello World!" > foo
  $ git add foo
  $ export DATE="2016-08-21 17:18:43 +0200"
  $ export GIT_COMMITTER_DATE="2016-08-21 17:18:43 +0200"
  $ git commit --date "$DATE" -q -m .
  $ cd ..
  $ git daemon --base-path=. --export-all --reuseaddr --pid-file=pid --detach
  $ touch git-daemon-export-ok
  $ mgit git://localhost/simple <<EOF
  > get /foo
  > save db.pack
  > quit
  > EOF
  00000000: 4865 6c6c 6f20 576f 726c 6421 0a         Hello World!.
  $ tail -c20 db.pack | hxd.xxd
  00000000: e4b2 3437 2e7e 3d7e 8508 3912 3d87 11cd  ..47.~=~..9.=...
  00000010: 9942 7147                                .BqG
  $ mgit git://localhost/simple db.pack <<EOF
  > get /foo
  > quit
  > EOF
  00000000: 4865 6c6c 6f20 576f 726c 6421 0a         Hello World!.
  $ cd simple
  $ echo "Git rocks!" > bar
  $ git add bar
  $ git commit --date "$DATE" -q -m .
  $ cd ..
  $ mgit git://localhost/simple db.pack <<EOF
  > pull
  > get /bar
  > get /foo
  > quit
  > EOF
  + /bar
  * /
  00000000: 4769 7420 726f 636b 7321 0a              Git rocks!.
  00000000: 4865 6c6c 6f20 576f 726c 6421 0a         Hello World!.
  $ kill $(cat pid)
