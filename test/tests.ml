let ( let* ) = Result.bind

let run_it cmd =
  let* status = Bos.OS.Cmd.run_status cmd in
  if status = `Exited 0 then Ok () else
    Error (`Msg ("status not 0, but " ^ Fmt.to_to_string Bos.OS.Cmd.pp_status status))

let empty_repo () =
  let* cwd = Bos.OS.Dir.current () in
  let* tmpdir = Bos.OS.Dir.tmp "git-kv-%s" in
  let* () = Bos.OS.Dir.set_current tmpdir in
  let cmd = Bos.Cmd.(v "git" % "init" % "-q") in
  let* () = run_it cmd in
  let* () = Bos.OS.Dir.set_current cwd in
  let cmd = Bos.Cmd.(v "git" % "daemon" % "--base-path=." % "--export-all" % "--reuseaddr" % "--pid-file=pid" % "--detach") in
  let* () = run_it cmd in
  let* pid = Bos.OS.File.read (Fpath.v "pid") in
  Ok (tmpdir, String.trim pid)

let kill_git pid =
  Unix.kill (int_of_string pid) Sys.sigterm

let simple () =
  match
    let* (tmpdir, pid) = empty_repo () in
    print_endline ("git started with " ^ Fpath.to_string tmpdir);
    print_endline ("git pid " ^ pid);
    Unix.sleep 2;
    kill_git pid;
    print_endline "git killed";
    Ok ()
  with
  | Ok () -> Alcotest.(check bool __LOC__ true true)
  | Error `Msg msg ->
    print_endline ("got an error from bos: " ^ msg)

let basic_tests = [
  "Simple", `Quick, simple ;
]

let tests = [
  "Basic tests", basic_tests ;
]

let () = Alcotest.run "Git-KV alcotest tests" tests
