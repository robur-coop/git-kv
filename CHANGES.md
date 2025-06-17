# v0.2.0 2025-06-17 - Paris (France)

- Delete the `ocaml-git` dependency (@dinosaure, @hannesm, !13)
- Be able to handle links (@reynir, !14)

# v0.1.3 2025-03-10 - Berlin (Germany)

- Defunctorise: remove functor over PCLOCK, use mirage-ptime instead of mirage-clock (!12 @hannesm, review by @reynir)

# v0.1.2 2024-12-20 - Vig (Denmark)

- example app/mgit.exe: initialize the RNG and add an mtime repl command (!9 - @reynir, review by @hannesm)
- **BUG FIX**: make `last_modified` return the commit date when on a clean commit.
  This restores the behavior before v0.1.0 (!10 - @reynir, review by @hannesm, @dinosaure)

# v0.1.1 2024-11-28 - Vig (Denmark)

- Remove unused code from test setup (!4 - @hannesm)
- **BUG FIX**: of_octets: create ring buffer earlier to avoid missing data (!5 - @hanensm, review by @dinosaure @reynir)
- Add function `commit : t -> Digestif.SHA1.t option` (!6 !8 - @hannesm @reynir, review by @dinosaure)

# v0.1.0 2024-10-29 - Berlin (Germany) and Bamenda (Cameroon)

- Refine change_and_push semantics (fixing on GitHub #1 #2 - @reynir @dinosaure @hannesm git.robur.coop #2)
  - only a single task may execute it at once (using a Lwt_mutex.t to protect this)
  - abort if the state is updated in parallel by a different task while change_and_push is executing
- Do the rename inside a change_and_push (so only a single commit from a rename), git.robur.coop #3
- Add alcotest unit tests
- Fix CRAM tests

# v0.0.5 2024-05-17 - Paris (France)

- Return a raw representation of the hash instead of the hex representation (@hannesm, #35)
- Stream in & out {of,to}_octets instead of manipulating the whole PACK file (@dinosaure, #33)

# v0.0.4 2024-04-24 - Paris (France)

- Add a branch accessor (@dinosaure, #30)
- Add the compression level argument into `to_octets` (@hannesm, @dinosaure, #31)
- Add few `Lwt.pause` to be cooperative with processes (@hannesm, @dinosaure, #32)

# v0.0.3 2022-12-16 - Berlin (Germany)

- Allow author, author_email, and message being specified in `change_and_push`
  (#28 @hannes)

# v0.0.2 2022-12-14 - Berlin (Germany)

- Use Stdlib.Result and Fmt instead of Rresult (#24 @hannes)
- Use Git.Reference.main (available since git 3.10.0, #24 @hannes)
- change_and_push may return errors, report them (#24 @hannes)
- delete unix transitive dependencies from tests (#25 @dinosaure)
- update to mirage-kv 6.0.0 (#27 @hannes)

# v0.0.1 2022-11-02 - Paris (France)

- First release
