module SHA1 = Digestif.SHA1

let ( <.> ) f g = fun x -> f (g x)

type configuration = {
  stateless: bool;
  mutable multi_ack: [ `None | `Some | `Detailed ];
  no_done: bool;
}

type 'uid hex = {
  to_hex: 'uid -> string;
  of_hex: string -> 'uid;
  compare: 'uid -> 'uid -> int;
}

(* Constants defined by the canoncial git implementation in C *)
let initial_flush = 16
let max_in_vain = 256
let large_flush = 16384
let pipe_safe_flush = 32

(* XXX(dinosaure): this part is really **ugly**! But we must follow the same
   behaviour of [git]. Instead to understand the synchronisation process of [git]
   with Smart.v1 and implement a state of the art synchronisation algorithm, I
   translated as is [fetch-pack.c:find_common] in OCaml. *)

let tips ((store, _) as t) negotiator =
  let open Lwt.Infix in
  Git_store.Ref.list store
  >>= Lwt_list.map_p (Lwt.return <.> fst)
  >>= Lwt_list.fold_left_s
        (fun () reference ->
          Git_mstore.deref t reference
          >>= Option.fold ~none:(Lwt.return None) ~some:(Git_mstore.get t)
          >|= Option.iter (fun obj -> Default.tip negotiator obj))
        ()

let consume_shallow_list flow cfg deepen ctx =
  let open Lwt in
  if cfg.stateless && Option.is_some deepen then
    Smart_flow.run flow Smart.(recv ctx shallows) >|= fun shallows ->
    List.map (Smart.Shallow.map ~fn:SHA1.of_hex) shallows
  else Lwt.return []

let handle_shallow flow store ctx =
  let open Lwt.Infix in
  Smart_flow.run flow Smart.(recv ctx shallows) >>= fun shallows ->
  let shallows = List.map (Smart.Shallow.map ~fn:SHA1.of_hex) shallows in
  Lwt_list.fold_left_s
    (fun () -> function
      | Smart.Shallow.Shallow uid -> Git_store.shallow store uid
      | Unshallow uid -> Git_store.unshallow store uid)
    () shallows

let unsafe_write_have ctx hex =
  let packet = Fmt.str "have %s\n" hex in
  Smart.Unsafe.write ctx packet

let next_flush stateless count =
  if stateless then if count < large_flush then count lsl 1 else count * 11 / 10
  else if count < pipe_safe_flush then count lsl 1
  else count + pipe_safe_flush

let find_common
    flow
    cfg
    ((store, _mem) as t)
    negotiator
    ctx
    ?(deepen : [ `Depth of int | `Timestamp of int64 ] option)
    refs =
  let open Lwt.Infix in
  let {stateless; no_done; _} = cfg in

  let fold acc remote_uid =
    Git_mstore.get t remote_uid >|= function
    | Some _ -> acc
    | None -> (remote_uid, ref 0) :: acc
  in

  Lwt_list.fold_left_s fold [] refs
  >|= List.sort_uniq (fun (a, _) (b, _) -> compare a b)
  >>= function
  | [] ->
    Smart_flow.run flow Smart.(send ctx flush ()) >>= fun () ->
    Lwt.return `Close
  | (uid, _) :: others ->
    Git_store.shallowed store >>= fun shallowed ->
    let shallowed = List.map SHA1.to_hex shallowed in
    Smart_flow.run flow
      Smart.(
        let uid = SHA1.to_hex uid in
        let others = List.map (fun (uid, _) -> SHA1.to_hex uid) others in
        let {Smart.Context.my_caps; _} = Smart.Context.capabilities ctx in
        let deepen =
          (deepen
            :> [ `Depth of int | `Not of string | `Timestamp of int64 ] option)
        in
        send ctx send_want
          (Want.v ~capabilities:my_caps ~shallows:shallowed ?deepen
             (uid :: others)))
    >>= fun () ->
    (match deepen with
    | None -> Lwt.return ()
    | Some _ -> handle_shallow flow store ctx)
    >>= fun () ->
    let in_vain = ref 0 in
    let count = ref 0 in
    let flush_at = ref initial_flush in
    let flushes = ref 0 in
    let got_continue = ref false in
    let got_ready = ref false in
    let retval = ref (-1) in
    (* TODO(dinosaure): handle [shallow] and [unshallow]. *)
    let rec go negotiator =
      Default.next ~parents:Git_mstore.parents t negotiator >>= function
      | None -> Lwt.return ()
      | Some uid ->
        unsafe_write_have ctx (SHA1.to_hex uid);
        (* completely unsafe! *)
        incr in_vain;
        incr count;
        if !flush_at <= !count then (
          Smart_flow.run flow Smart.(send ctx flush ()) >>= fun () ->
          incr flushes;
          flush_at := next_flush stateless !count;
          if (not stateless) && !count = initial_flush then go negotiator
          else
            consume_shallow_list flow cfg None ctx >>= fun _shallows ->
            let rec loop () =
              Smart_flow.run flow Smart.(recv ctx recv_ack)
              >|= Smart.Negotiation.map ~fn:SHA1.of_hex
              >>= fun ack ->
              match ack with
              | Smart.Negotiation.NAK -> Lwt.return `Continue
              | ACK _ ->
                flushes := 0;
                cfg.multi_ack <- `None;
                (* XXX(dinosaure): [multi_ack] supported by the client but it
                         is not supported by the server. TODO: use [Context.shared]. *)
                retval := 0;
                Lwt.return `Done
              | ACK_common uid | ACK_ready uid | ACK_continue uid -> (
                Git_mstore.get t uid >>= function
                | None -> assert false
                | Some obj ->
                  Default.ack ~parents:Git_mstore.parents t negotiator obj
                  >>= fun was_common ->
                  if
                    stateless
                    && Smart.Negotiation.is_common ack
                    && not was_common
                  then (
                    (* we need to replay the have for this object on the next RPC request so
                               the peer kows it is in common with us. *)
                    unsafe_write_have ctx (SHA1.to_hex uid);
                    (* reset [in_vain] because an ack for this commit has not been seen. *)
                    in_vain := 0;
                    retval := 0;
                    got_continue := true;
                    loop ())
                  else if
                    (not stateless) || not (Smart.Negotiation.is_common ack)
                  then (
                    in_vain := 0;
                    retval := 0;
                    got_continue := true;
                    if Smart.Negotiation.is_ready ack then got_ready := true;
                    loop ())
                  else (
                    retval := 0;
                    got_continue := true;
                    if Smart.Negotiation.is_ready ack then got_ready := true;
                    loop ()))
            in
            loop () >>= function
            | `Done -> Lwt.return ()
            | `Continue ->
              decr flushes;
              if !got_continue && max_in_vain < !in_vain then Lwt.return ()
              else if !got_ready then Lwt.return ()
              else go negotiator)
        else go negotiator
    in
    go negotiator >>= fun () ->
    (if (not !got_ready) || not no_done then
       Smart_flow.run flow Smart.(send ctx negotiation_done ())
     else Lwt.return ())
    >>= fun () ->
    if !retval <> 0 then (
      cfg.multi_ack <- `None;
      incr flushes);
    (if (not !got_ready) || not no_done then
       Smart_flow.run flow Smart.(recv ctx shallows) >>= fun _shallows ->
       Lwt.return ()
     else Lwt.return ())
    >>= fun () ->
    let rec go () =
      if !flushes > 0 || cfg.multi_ack = `Some || cfg.multi_ack = `Detailed then (
        Smart_flow.run flow Smart.(recv ctx recv_ack)
        >|= Smart.Negotiation.map ~fn:SHA1.of_hex
        >>= fun ack ->
        match ack with
        | Smart.Negotiation.ACK _ -> Lwt.return (`Continue 0)
        | ACK_common _ | ACK_continue _ | ACK_ready _ ->
          cfg.multi_ack <- `Some;
          go ()
        | NAK -> decr flushes; go ())
      else if !count > 0 then Lwt.return (`Continue !retval)
      else Lwt.return (`Continue 0)
    in
    go ()
