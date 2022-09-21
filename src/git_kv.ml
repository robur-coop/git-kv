
module Store = Git.Mem.Make(Digestif.SHA1)
module Sync = Git.Mem.Sync(Store)
module Search = Git.Search.Make(Digestif.SHA1)(Store)
module Git_commit = Git.Commit.Make(Store.Hash)

type t = {
  ctx : Mimic.ctx ;
  edn : Smart_git.Endpoint.t;
  branch : Git.Reference.t ;
  store : Store.t ;
  mutable head : Store.hash option ;
}

let init_store () =
  let open Lwt.Infix in
  Store.v (Fpath.v ".") >|= fun r ->
  Result.map_error
    (fun e -> `Msg (Fmt.str "error setting up store %a" Store.pp_error e))
    r

let main = Git.Reference.v "refs/heads/main"

let capabilities =
  [ `Side_band_64k; `Multi_ack_detailed; `Ofs_delta; `Thin_pack; `Report_status ]

let to_invalid = function
  | Ok x -> x
  | Error `Msg m -> invalid_arg m

let split_url s =
  match String.split_on_char '#' s with
  | [ edn; branch ] ->
    Smart_git.Endpoint.of_string edn |> to_invalid,
    Git.Reference.of_string branch |> to_invalid
  | _ ->
    Smart_git.Endpoint.of_string s |> to_invalid, main

let pull t =
  let open Lwt.Infix in
  Sync.fetch ~capabilities ~ctx:t.ctx t.edn t.store ~deepen:(`Depth 1) `All >>= fun r ->
  let data =
    Result.map_error
      (fun e -> `Msg (Fmt.str "error fetching: %a" Sync.pp_error e))
      r
  in
  match data with
  | Error _ as e -> Lwt.return e
  | Ok None -> Lwt.return (Ok [])
  | Ok Some (_, _) ->
    Store.Ref.resolve t.store t.branch >>= fun r ->
    let head =
      Result.map_error
        (fun e -> `Msg (Fmt.str "error resolving branch %s: %a"
                          (Git.Reference.to_string t.branch)
                          Store.pp_error e))
        r |> to_invalid
    in
    t.head <- Some head;
    Lwt.return (Ok [])

let connect ctx endpoint =
  let open Lwt.Infix in
  init_store () >>= fun store ->
  let store = to_invalid store in
  let edn, branch = split_url endpoint in
  let t = { ctx ; edn ; branch ; store ; head = None } in
  pull t >>= fun r ->
  let _r = to_invalid r in
  Lwt.return t

type key = Mirage_kv.Key.t

type change = [
  | `Add of key
  | `Remove of key
  | `Change of key
]

let pull _ = assert false

type error = Mirage_kv.error

let pp_error ppf = Mirage_kv.pp_error ppf

let disconnect _t = Lwt.return_unit

let to_octets t =
  (* TODO maybe preserve edn and branch as well? *)
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return ""
  | Some head ->
    Store.read_exn t.store head >|= function
    | Commit c ->
      let l = Encore.to_lavoisier Git_commit.format in
      Encore.Lavoisier.emit_string c l
    | _ -> assert false

let of_octets ctx ~remote data =
  (* TODO maybe recover edn and branch from data as well? *)
  let open Lwt_result.Infix in
  let l = Encore.to_angstrom Git_commit.format in
  Lwt.return
    (Result.map_error (fun e -> `Msg e)
       (Angstrom.parse_string ~consume:All l data)) >>= fun head ->
  let head = Git_commit.tree head in
  init_store () >|= fun store ->
  let edn, branch = split_url remote in
  { ctx ; edn ; branch ; store ; head = Some head }

let exists t key =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return (Ok None)
  | Some head ->
    Search.mem t.store head (`Path (Mirage_kv.Key.segments key)) >>= function
    | false -> Lwt.return (Ok None)
    | true ->
      Search.find t.store head (`Path (Mirage_kv.Key.segments key))
      >|= Option.get >>= Store.read_exn t.store >>= function
      | Blob _ -> Lwt.return (Ok (Some `Value))
      | Tree _ | Commit _ | Tag _ -> Lwt.return (Ok (Some `Dictionary))

let get t key =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return (Error (`Not_found key))
  | Some head ->
    Search.find t.store head (`Path (Mirage_kv.Key.segments key)) >>= function
    | None -> Lwt.return (Error (`Not_found key))
    | Some blob ->
      Store.read_exn t.store blob >|= function
      | Blob b -> Ok (Git.Blob.to_string b)
      | _ -> assert false

let get_partial t key ~offset ~length =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  if String.length data < offset then
    ""
  else
    let l = min length (String.length data - offset) in
    String.sub data offset l

let list _t _key =
  (* ((string * [`Value | `Dictionary]) list, error) result Lwt.t *)
  assert false

let last_modified t key =
  let open Lwt.Infix in
  Option.fold
    ~none:(Lwt.return (Error (`Not_found key)))
    ~some:(fun head ->
        Store.read_exn t.store head >|= function
        | Commit c ->
          let author = Git_commit.author c in
          let secs, tz_offset = author.Git.User.date in
          let secs =
            Option.fold ~none:secs
              ~some:(fun { Git.User.sign ; hours ; minutes } ->
                  let tz_off = Int64.(mul (add (mul (of_int hours) 60L) (of_int minutes)) 60L) in
                  match sign with
                  | `Plus -> Int64.(sub secs tz_off)
                  | `Minus -> Int64.(add secs tz_off))
              tz_offset
          in
          let ts =
            Option.fold ~none:Ptime.epoch ~some:Fun.id (Ptime.of_float_s (Int64.to_float secs))
          in
          Ok (Ptime.(Span.to_d_ps (to_span ts)))
        | _ -> assert false)
    t.head

let digest t key =
  Option.fold
    ~none:(Error (`Not_found key))
    ~some:(fun x -> Ok (Store.Hash.to_hex x))
    t.head |> Lwt.return

let size t key =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  String.length data
