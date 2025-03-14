# Git-kv, a simple Key-Value store synchronized with a Git repository

This library is a simple implementation of a Git repository that can be read
and/or modified. It offers two ways to create such a local repository:
1) The local repository can be created in a serialized state
2) The local repository can be created from a remote repository

The first method has the advantage of not requiring an internet connection. The
serialized state can be created with the `mgit` tool:
```sh
$ mgit https://github.com/mirage/mirage <<EOF
> save db.pack
> quit
$ ls db.pack
db.pack
```

The disadvantage is that the serialized state may be out of sync with the state
of the remote repository. In this case, the user has access to the `pull`
function, which allows the internet state of the local repository to be
re-synchronised with the remote repository.
```ocaml
let contents_of_file filename =
  let ic = open_in filename in
  let ln = in_channel_length ic in
  let rs = Bytes.create ln in
  really_input ic rs 0 ln ;
  Bytes.unsafe_to_string rs

let _ =
  Git_kv.of_octets ctx 
    ~remote:"git@github.com:mirage/mirage.git"
    (contents_of_file "db.pack") >>= fun t ->
  Git_kv.pull t >>= fun diff ->
  ...
```

The second method initiates a connection to the remote repository in order to
download its state and reproduce a synchronised internal state. The type of
connections supported are described in the given `ctx`. We recommend the
tutorial about [Mimic][mimic] to understand its use.
```sh
let _ =
  Git_kv.connect ctx "git@github.com:mirage/mirage.git" >>= fun t ->
  ...
```

The user can manipulate the repository as an [RW][mirage-kv-rw] repository. Any
change to the repository requires a new commit. These changes will be sent to
the remote repository. The user can _fold_ any changes into one commit if
he/she wants.
```ocaml
let new_file t =
  Git_kv.set t Mirage_kv.Key.(empty / "foo") "foo" >>= fun () ->
  (* XXX(dinosaure): a commit was created and sended to the
     remote repository. *)
  ...

let new_files t =
  Git_kv.change_and_push t @@ fun t ->
  Git_kv.set t Mirage_kv.Key.(empty / "foo" "foo") >>= fun () ->
  Git_kv.set t Mirage_kv.Key.(empty / "bar" "bar")
(* XXX(dinosaure): multiple files are added into the local repository
   but they are committed only at the end of the given function
   to [change_and_push]. That's say, only one commit was made and sended to the
   remote Git repository. *)
```

[mimic]: https://dinosaure.github.io/mimic/mimic/index.html
[mirage-kv-rw]: https://github.com/mirage/mirage-kv
