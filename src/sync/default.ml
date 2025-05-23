(** default[1] negotiator implementation

    [1] "default" as defined in the canonical git implementation in C, see
    https://github.com/git/git/tree/master/negotiator *)

module SHA1 = Digestif.SHA1

type ('k, 'p, 't) psq =
  (module Psq.S with type k = 'k and type p = 'p and type t = 't)

type t =
  | State : {
      mutable rev_list: 'psq;
      psq: (SHA1.t, SHA1.t * int ref * int64, 'psq) psq;
      mutable non_common_revs: int;
    }
      -> t

let _common = 1 lsl 2
let _common_ref = 1 lsl 3
let _seen = 1 lsl 4
let _popped = 1 lsl 5

let make =
 fun ~compare ->
  let module K = struct
    type t = SHA1.t

    let compare = compare
  end in
  let module P = struct
    type t = SHA1.t * int ref * int64

    let compare (_, _, a) (_, _, b) = Int64.compare b a
  end in
  let module Psq = Psq.Make (K) (P) in
  let rev_list = Psq.empty in
  let non_common_revs = 0 in
  State {rev_list; psq= (module Psq); non_common_revs}

let rev_list_push =
 fun (State ({rev_list; psq= (module Psq); non_common_revs} as state))
     (uid, p, ts) mark ->
  if !p land mark = 0 then p := !p lor mark;
  state.rev_list <- Psq.add uid (uid, p, ts) rev_list;
  if !p land _common = 0 then state.non_common_revs <- non_common_revs + 1

let rec mark_common =
 fun ~parents store (State ({non_common_revs; _} as state) as t) (uid, p, ts)
     only_ancestors ->
  let ( >>= ) = Lwt.bind in

  if only_ancestors then p := !p lor _common;
  if !p land _seen = 0 then (
    rev_list_push t (uid, p, ts) _seen;
    Lwt.return ())
  else (
    if (not only_ancestors) && !p land _popped = 0 then
      state.non_common_revs <- non_common_revs - 1;
    parents store uid
    >>=
    let rec go = function
      | [] -> Lwt.return ()
      | (uid, p, ts) :: rest ->
        mark_common ~parents store t (uid, p, ts) false >>= fun () -> go rest
    in
    go)

let known_common =
 fun ~parents store t (uid, p, ts) ->
  if !p land _seen = 0 then (
    rev_list_push t (uid, p, ts) (_common_ref lor _seen);
    mark_common ~parents store t (uid, p, ts) true)
  else Lwt.return ()

let tip t obj = rev_list_push t obj _seen

let ack =
 fun ~parents store t (uid, p, ts) ->
  let ( >>= ) = Lwt.bind in

  let res = not (!p land _common = 0) in
  mark_common ~parents store t (uid, p, ts) false >>= fun () -> Lwt.return res

let get_rev =
 fun ~parents store (State ({psq= (module Psq); _} as state) as t) ->
  let ( >>= ) = Lwt.bind in

  let rec go () =
    if state.non_common_revs = 0 || Psq.is_empty state.rev_list then
      Lwt.return None
    else
      match Psq.pop state.rev_list with
      | None -> Lwt.return None
      | Some ((uid, (_, p, _)), rev_list) ->
        state.rev_list <- rev_list;
        parents store uid >>= fun ps ->
        p := !p lor _popped;
        if !p land _common = 0 then
          state.non_common_revs <- state.non_common_revs - 1;

        let mark = ref 0 in
        let res = ref (Some uid) in

        if !p land _common <> 0 then (
          mark := _common lor _seen;
          res := None)
        else if !p land _common_ref <> 0 then mark := _common lor _seen
        else mark := _seen;

        let rec loop = function
          | [] -> (
            match !res with None -> go () | Some _ as v -> Lwt.return v)
          | (uid, p, ts) :: rest ->
            if !p land _seen = 0 then rev_list_push t (uid, p, ts) !mark;

            if !mark land _common <> 0 then
              mark_common ~parents store t (uid, p, ts) true >>= fun () ->
              loop rest
            else loop rest
        in
        loop ps
  in
  go ()

let next = fun ~parents store t -> get_rev ~parents store t
