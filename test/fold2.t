Reading during batch operation
  $ mkdir simple
  $ git init --bare -q simple 2> /dev/null
  $ git daemon --base-path=. --export-all --enable=receive-pack --reuseaddr --pid-file=pid --detach
  $ touch git-daemon-export-ok
  $ mgit git://localhost/simple#main << EOF
  > fold
  > set /bar "Git rocks!"
  > list /
  > get /bar
  > quit
  > quit
  > EOF
  - /bar
  00000000: 4769 7420 726f 636b 7321                 Git rocks!
  $ cd simple
  $ git log main --pretty=oneline | wc -l | tr -d ' '
  1
  $ cd ..
  $ kill $(cat pid)
