(* (c) 2015 Daniel C. Bünzli
 * (c) 2020 Romain Calascibetta
 *
 * This implementation differs a bit from [fpath]
 * where absolute path is not valid and we manipulate
 * only POSIX path - the backend takes care about Windows
 * and POSIX paths then. *)

module SHA1 = Digestif.SHA1

type t = string (* non empty *)

let dir_sep = "/"
let dir_sep_char = '/'
let error_msgf fmt = Fmt.kstr (fun err -> Error (`Msg err)) fmt

let validate_and_collapse_seps p =
  let max_idx = String.length p - 1 in
  let rec with_buf b last_sep k i =
    if i > max_idx then Ok (Bytes.sub_string b 0 k)
    else
      let c = p.[i] in
      if c = '\x00' then error_msgf "Malformed reference: %S" p
      else if c <> dir_sep_char then (
        Bytes.set b k c;
        with_buf b false (k + 1) (i + 1))
      else if not last_sep then (
        Bytes.set b k c;
        with_buf b true (k + 1) (i + 1))
      else with_buf b true k (i + 1)
  in
  let rec try_no_alloc last_sep i =
    if i > max_idx then Ok p
    else
      let c = p.[i] in
      if c = '\x00' then error_msgf "Malformed reference: %S" p
      else if c <> dir_sep_char then try_no_alloc false (i + 1)
      else if not last_sep then try_no_alloc true (i + 1)
      else
        let b = Bytes.of_string p in
        with_buf b true i (i + 1)
  in
  let start =
    if max_idx > 0 then if p.[0] = dir_sep_char then 1 else 0 else 0
  in
  try_no_alloc false start

let of_string p =
  if p = "" then error_msgf "Empty path"
  else
    match validate_and_collapse_seps p with
    | Ok p ->
      if p.[0] = dir_sep_char then error_msgf "Absolute reference" else Ok p
    | Error _ as err -> err

let v p =
  match of_string p with Ok v -> v | Error (`Msg err) -> invalid_arg err

let is_seg s =
  let zero = String.contains s '\x00' in
  let sep = String.contains s dir_sep_char in
  (not zero) && not sep

let add_seg p seg =
  if not (is_seg seg) then Fmt.invalid_arg "Invalid segment: %S" seg;
  let sep = if p.[String.length p - 1] = dir_sep_char then "" else dir_sep in
  String.concat sep [p; sep]

let append p0 p1 =
  if p1.[0] = dir_sep_char then p1
  else
    let sep =
      if p0.[String.length p0 - 1] = dir_sep_char then "" else dir_sep
    in
    String.concat sep [p0; p1]

let ( / ) p seg = add_seg p seg
let ( // ) p0 p1 = append p0 p1
let segs p = String.split_on_char dir_sep.[0] p
let pp ppf p = Fmt.string ppf p
let to_string x = x
let equal p0 p1 = String.equal p0 p1
let compare p0 p1 = String.compare p0 p1
let head = "HEAD"
let master = "refs/heads/master"
let main = "refs/heads/main"

module Ordered = struct
  type nonrec t = t

  let compare a b = compare a b
end

module Map = Map.Make (Ordered)
module Set = Set.Make (Ordered)

type contents = Uid of SHA1.t | Ref of t

let equal_contents ~equal:equal_uid a b =
  match a, b with
  | Uid a, Uid b -> equal_uid a b
  | Ref a, Ref b -> equal a b
  | _ -> false

let pp_contents ~pp:pp_uid ppf = function
  | Ref v -> pp ppf v
  | Uid v -> pp_uid ppf v

let compare_contents ~compare:compare_uid a b =
  let inf = -1 and sup = 1 in
  match a, b with
  | Ref a, Ref b -> compare a b
  | Uid a, Uid b -> compare_uid a b
  | Ref _, _ -> sup
  | Uid _, _ -> inf

module Packed = struct
  type elt = Ref of t * SHA1.t | Peeled of SHA1.t
  type t = elt list
  type 'fd input_line = 'fd -> string option Lwt.t

  let load ~input_line ~of_hex fd =
    let ( let* ) = Lwt.bind in
    let rec go acc =
      let* line = input_line fd in
      match line with
      | Some "" -> go acc
      | Some line -> begin
        match line.[0] with
        | '#' -> go acc
        | '^' ->
          let uid = String.sub line 1 (String.length line - 1) in
          let uid = of_hex uid in
          go (Peeled uid :: acc)
        | _ -> (
          match String.split_on_char ' ' line with
          | [] | [_] -> go acc
          | uid :: reference ->
            let reference = v (String.concat " " reference) in
            let uid = of_hex uid in
            go (Ref (reference, uid) :: acc))
      end
      | None -> Lwt.return (List.rev acc)
    in
    go []

  exception Found

  let exists reference packed =
    let res = ref false in
    let f = function
      | Ref (reference', _) ->
        if equal reference reference' then begin
          res := true;
          raise Found
        end
      | _ -> ()
    in
    (try List.iter f packed with Found -> ());
    !res

  let get reference packed =
    let res = ref None in
    let f = function
      | Ref (reference', uid) ->
        if equal reference reference' then begin
          res := Some uid;
          raise Found
        end
      | _ -> ()
    in
    (try List.iter f packed with Found -> ());
    !res

  let remove reference packed =
    let fold acc = function
      | Ref (reference', uid) ->
        if equal reference reference' then acc else Ref (reference', uid) :: acc
      | v -> v :: acc
    in
    List.rev (List.fold_left fold [] packed)
end

type store = {expanded: (t, string) Hashtbl.t; packed: Packed.t}
(* NOTE(dinosaure): [packed] is not really used and it's an old artifact from
   [ocaml-git] about packed references which can exist into a file-system. *)

let reword_error f = function Ok _ as o -> o | Error err -> Error (f err)

let contents _store str =
  match SHA1.of_hex_opt (String.trim str) with
  | Some uid -> Uid uid
  | None -> begin
    match String.split_on_char ' ' str with
    | _ref :: value -> Ref (v (String.concat " " value))
    | _ -> Fmt.invalid_arg "Invalid reference contents: %S" str
  end

let resolve t store reference =
  let rec go visited reference =
    let res = Hashtbl.find_opt t.expanded reference in
    match res with
    | None -> begin
      match Packed.get reference store.packed with
      | Some uid -> Lwt.return (Ok uid)
      | None -> Lwt.return (Error (`Not_found reference))
    end
    | Some str -> begin
      match contents store str with
      | Uid uid -> Lwt.return (Ok uid)
      | Ref reference ->
        if List.exists (equal reference) visited then Lwt.return (Error `Cycle)
        else go (reference :: visited) reference
    end
  in
  go [reference] reference

let read t store reference =
  match Hashtbl.find_opt t.expanded reference with
  | None -> begin
    match Packed.get reference store.packed with
    | Some uid -> Lwt.return (Ok (Uid uid))
    | None -> Lwt.return (Error (`Not_found reference))
  end
  | Some str -> Lwt.return (Ok (contents store str))

let write t reference contents =
  let str =
    match contents with
    | Uid uid -> Fmt.str "%s\n" (SHA1.to_hex uid)
    | Ref t -> Fmt.str "ref: %s\n" t
  in
  Hashtbl.replace t.expanded reference str;
  Lwt.return (Ok ())

let uid uid = Uid uid
let ref t = Ref t
