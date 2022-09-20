
module Store = Git.Mem.Make(Digestif.SHA1)
module Sync = Git.Mem.Sync(Store)
module Search = Git.Search.Make(Digestif.SHA1)(Store)
module Git_commit = Git.Commit.Make(Store.Hash)

type t = {
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

let connect ctx endpoint =
  let open Lwt.Infix in
  init_store () >>= fun store ->
  let store = to_invalid store in
  let edn, branch = split_url endpoint in
  Sync.fetch ~capabilities ~ctx edn store ~deepen:(`Depth 1) `All >>= fun r ->
  let data =
    Result.map_error
      (fun e -> `Msg (Fmt.str "error fetching: %a" Sync.pp_error e))
      r |> to_invalid
  in
  match data with
  | None -> Lwt.return { store ; head = None }
  | Some (_, _) ->
    Store.Ref.resolve store branch >>= fun r ->
    let head =
      Result.map_error
        (fun e -> `Msg (Fmt.str "error resolving branch %s: %a"
                          (Git.Reference.to_string branch) Store.pp_error e))
        r |> to_invalid
    in
    Lwt.return { store ; head = Some head }

type error = Mirage_kv.error

let pp_error ppf = Mirage_kv.pp_error ppf

let disconnect _t = Lwt.return_unit

let to_octets t =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return ""
  | Some head ->
    Store.read_exn t.store head >|= function
    | Commit c ->
      let l = Encore.to_lavoisier Git_commit.format in
      Encore.Lavoisier.emit_string c l
    | _ -> assert false

let of_octets data =
  let open Lwt_result.Infix in
  let l = Encore.to_angstrom Git_commit.format in
  Lwt.return
    (Result.map_error (fun e -> `Msg e)
       (Angstrom.parse_string ~consume:All l data)) >>= fun head ->
  let head = Git_commit.tree head in
  init_store () >|= fun store ->
  { store ; head = Some head }

type key = Mirage_kv.Key.t

let exists _t _key =
  (*  Search.find t.store t.head (`Path (Mirage_kv.Key.segments key)) >>= function *)
  (* ([`Value | `Dictionary] option, error) result Lwt.t *)
  assert false

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

let last_modified _t _key =
  (* (int * int64, error) result Lwt.t *)
  assert false

let digest t _key =
  Lwt.return (Ok (Option.fold ~none:"0" ~some:Store.Hash.to_hex t.head))

let size t key =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  String.length data
