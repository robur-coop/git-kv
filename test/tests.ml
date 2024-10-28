let ( let* ) = Result.bind

let run_it cmd =
  let* status = Bos.OS.Cmd.run_status cmd in
  if status = `Exited 0 then Ok () else
    Error (`Msg ("status not 0, but " ^ Fmt.to_to_string Bos.OS.Cmd.pp_status status))

let empty_repo () =
  let* cwd = Bos.OS.Dir.current () in
  let* tmpdir = Bos.OS.Dir.tmp ~dir:cwd "git-kv-%s" in
  let* () = Bos.OS.Dir.set_current tmpdir in
  let cmd = Bos.Cmd.(v "git" % "init" % "--bare" % "-q") in
  let* () = run_it cmd in
  let* () = Bos.OS.Dir.set_current cwd in
  let cmd = Bos.Cmd.(v "git" % "daemon" % "--base-path=." % "--export-all" % "--reuseaddr" % "--pid-file=pid" % "--detach" % "--port=9419" % "--enable=receive-pack") in
  let* () = run_it cmd in
  let pid = Result.get_ok (Bos.OS.File.read (Fpath.v "pid")) in
  Ok (Fpath.basename tmpdir, String.trim pid)

let kill_git pid =
  Unix.kill (int_of_string pid) Sys.sigterm

module Store = Git_kv.Make (Pclock)

open Lwt.Infix

let read_in_change_and_push () =
  match
    let* (tmpdir, pid) = empty_repo () in
    Fun.protect ~finally:(fun () -> kill_git pid) (fun () ->
    Lwt_main.run
      (
        Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
        Store.change_and_push t (fun t ->
          Store.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure writing";
          Store.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure reading";
          Alcotest.(check string) "Store.get" "value" (Result.get_ok r);
          Lwt.return_unit) >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure change_and_push";
        Store.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure reading outside";
        Alcotest.(check string) "Store.get" "value" (Result.get_ok r);
        Lwt.return_unit));
    Ok ()
  with
  | Ok () -> ()
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)

let set_outside_change_and_push () =
  match
    let* (tmpdir, pid) = empty_repo () in
    Fun.protect ~finally:(fun () -> kill_git pid) (fun () ->
    Lwt_main.run
      (
        Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
        Store.change_and_push t (fun t' ->
          Store.set t' (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure writing foo";
          Store.set t (Mirage_kv.Key.v "/bar") "other value" >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure writing bar";
          Lwt.return_unit) >>= fun r ->
        if Result.is_ok r then Alcotest.fail "expected change_and_push failure";
        Store.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_ok r then Alcotest.fail "expected failure reading foo";
        Store.get t (Mirage_kv.Key.v "/bar") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure reading outside bar";
        Alcotest.(check string) "Store.get bar" "other value" (Result.get_ok r);
        Lwt.return_unit));
    Ok ()
  with
  | Ok () -> ()
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)

let remove_in_change_and_push () =
  match
    let* (tmpdir, pid) = empty_repo () in
    Fun.protect ~finally:(fun () -> kill_git pid) (fun () ->
    Lwt_main.run
      (
        Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
        Store.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure writing";
        Store.change_and_push t (fun t' ->
          Store.get t' (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure reading inside";
          Alcotest.(check string) "Store.get" "value" (Result.get_ok r);
          Store.remove t' (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure removing";
          Store.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure reading the outer t";
          Alcotest.(check string) "Store.get" "value" (Result.get_ok r);
          Lwt.return_unit) >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure change_and_push";
        Store.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_ok r then Alcotest.fail "expected failure reading outside";
        Lwt.return_unit));
    Ok ()
  with
  | Ok () -> ()
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)

let last_modified_in_change_and_push () =
  match
    let* (tmpdir, pid) = empty_repo () in
    Fun.protect ~finally:(fun () -> kill_git pid) (fun () ->
    Lwt_main.run
      (
        Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
        Store.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure writing";
        Store.last_modified t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure last_modified";
        let lm = Result.get_ok r in
        Store.change_and_push t (fun t' ->
            Store.last_modified t' (Mirage_kv.Key.v "/foo") >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure last_modified inside";
            let lm' = Result.get_ok r in
            Alcotest.(check bool) "last modified is later or equal" true (Ptime.is_later ~than:lm lm' || Ptime.equal lm lm');
            Store.set t' (Mirage_kv.Key.v "/foo") "new value" >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure writing inside";
            Store.last_modified t' (Mirage_kv.Key.v "/foo") >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure last_modified inside after set";
            let lm2 = Result.get_ok r in
            Alcotest.(check bool) "last modified is later" true (Ptime.is_later ~than:lm' lm2 || Ptime.equal lm' lm2);
            Lwt.return lm2) >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure change_and_push";
        let lm2 = Result.get_ok r in
        Store.last_modified t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure last_modified after change_and_push";
        let lm3 = Result.get_ok r in
        Alcotest.(check bool) "last modified is later" true (Ptime.is_later ~than:lm lm3 || Ptime.equal lm lm3);
        Alcotest.(check bool) "last modified is later outside than inside" true (Ptime.is_later ~than:lm2 lm3 || Ptime.equal lm2 lm3);
        Lwt.return_unit));
    Ok ()
  with
  | Ok () -> ()
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)

let digest_in_change_and_push () =
  match
    let* (tmpdir, pid) = empty_repo () in
    Fun.protect ~finally:(fun () -> kill_git pid) (fun () ->
    Lwt_main.run
      (
        Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
        Store.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure writing";
        Store.digest t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure digest";
        let digest = Result.get_ok r in
        Store.change_and_push t (fun t' ->
          Store.digest t' (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure digest inside";
          Alcotest.(check string) "Store.digest" digest (Result.get_ok r);
          Store.set t' (Mirage_kv.Key.v "/foo") "something else" >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure set";
          Store.digest t' (Mirage_kv.Key.v "/foo") >>= fun r ->
          if Result.is_error r then Alcotest.fail "failure digest inside";
          Alcotest.(check bool) "Store.digest" false (String.equal digest (Result.get_ok r));
          Lwt.return_unit) >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure change_and_push";
        Store.digest t (Mirage_kv.Key.v "/foo") >>= fun r ->
        if Result.is_error r then Alcotest.fail "failure digest outside";
        Alcotest.(check bool) "Store.digest" false (String.equal digest (Result.get_ok r));
        Lwt.return_unit));
    Ok ()
  with
  | Ok () -> ()
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)



let basic_tests = [
  "Read in change_and_push", `Quick, read_in_change_and_push ;
  "Set outside change_and_push", `Quick, set_outside_change_and_push ;
  "Remove in change_and_push", `Quick, remove_in_change_and_push ;
  "Last modified in change_and_push", `Quick, last_modified_in_change_and_push ;
  "Digest in change_and_push", `Quick, digest_in_change_and_push ;
]

let tests = [
  "Basic tests", basic_tests ;
]

let () = Alcotest.run "Git-KV alcotest tests" tests
