Batch operation
  $ mkdir simple
  $ cd simple
  $ git init --bare -q 2> /dev/null
  $ cd ..
  $ git daemon --base-path=. --export-all --enable=receive-pack --reuseaddr --pid-file=pid --detach
  $ touch git-daemon-export-ok
  $ mgit git://localhost/simple#main <<EOF
  > fold
  > set /bar "Git rocks!"
  > set /foo "Hello World!"
  > exists /bar
  > quit
  > quit
  > EOF
  /bar exists as a value
  $ mgit git://localhost/simple#main <<EOF
  > list /
  > get /bar
  > get /foo
  > quit
  > EOF
  - /bar
  - /foo
  00000000: 4769 7420 726f 636b 7321                 Git rocks!
  00000000: 4865 6c6c 6f20 576f 726c 6421            Hello World!
  $ cd simple
  $ git log main --pretty=oneline | wc -l | tr -d ' '
  1
  $ cd ..
  $ kill $(cat pid)
