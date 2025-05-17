(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 * and Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

module SHA1 = Digestif.SHA1

type kind = Blob | Commit | Tag | Tree

type t = {
  obj: SHA1.t;
  kind: kind;
  tag: string;
  tagger: Git_user.t option;
  message: string option;
}

let make target kind ?tagger ~tag message =
  {obj= target; kind; tag; tagger; message}

let pp_kind ppf = function
  | Blob -> Fmt.string ppf "Blob"
  | Commit -> Fmt.string ppf "Commit"
  | Tag -> Fmt.string ppf "Tag"
  | Tree -> Fmt.string ppf "Tree"

let pp ppf {obj; kind; tag; tagger; message} =
  Fmt.pf ppf
    "{ @[<hov>obj = %a;@ kind = %a;@ tag = %s;@ tagger = %a;@ message = %a@] }"
    SHA1.pp obj pp_kind kind tag
    (Fmt.hvbox (Fmt.option Git_user.pp))
    tagger
    Fmt.(option (hvbox text))
    message

let string_of_kind = function
  | Commit -> "commit"
  | Tag -> "tag"
  | Tree -> "tree"
  | Blob -> "blob"

module Syntax = struct
  let safe_exn f x = try f x with _ -> raise Encore.Bij.Bijection
  let hex = Encore.Bij.v ~fwd:(safe_exn SHA1.of_hex) ~bwd:(safe_exn SHA1.to_hex)

  let user =
    Encore.Bij.v
      ~fwd:(fun str ->
        match
          Angstrom.parse_string ~consume:Angstrom.Consume.All
            (Encore.to_angstrom Git_user.format)
            str
        with
        | Ok v -> v
        | Error _ -> raise Encore.Bij.Bijection)
      ~bwd:(fun v ->
        Encore.Lavoisier.emit_string v (Encore.to_lavoisier Git_user.format))

  let kind =
    Encore.Bij.v
      ~fwd:(function
        | "tree" -> Tree
        | "blob" -> Blob
        | "commit" -> Commit
        | "tag" -> Tag
        | _ -> raise Encore.Bij.Bijection)
      ~bwd:(function
        | Blob -> "blob" | Tree -> "tree" | Commit -> "commit" | Tag -> "tag")

  let tag =
    Encore.Bij.v
      ~fwd:(fun ((_, obj), (_, kind), (_, tag), tagger, message) ->
        {obj; kind; tag; tagger= Stdlib.Option.map snd tagger; message})
      ~bwd:(fun {obj; kind; tag; tagger; message} ->
        let tagger = Stdlib.Option.map (fun x -> "tagger", x) tagger in
        ("object", obj), ("type", kind), ("tag", tag), tagger, message)

  let is_not_sp chr = chr <> ' '
  let is_not_lf chr = chr <> '\x0a'
  let always x _ = x

  let rest =
    let open Encore.Syntax in
    let open Encore.Either in
    fix @@ fun m ->
    let cons = Encore.Bij.cons <$> (while0 (always true) <* commit <*> m) in
    let nil = pure ~compare:(fun () () -> true) () in
    Encore.Bij.v
      ~fwd:(function L cons -> cons | R () -> [])
      ~bwd:(function _ :: _ as lst -> L lst | [] -> R ())
    <$> peek cons nil

  let rest : string Encore.t =
    let open Encore.Syntax in
    Encore.Bij.v ~fwd:(String.concat "") ~bwd:(fun x -> [x]) <$> rest

  let rest =
    let open Encore.Syntax in
    let open Encore.Either in
    let fwd = function L str -> Some str | R _ -> None in
    let bwd = function Some str -> L str | None -> R "" in
    map (Encore.Bij.v ~fwd ~bwd)
      (peek ((Encore.Bij.char '\x0a' <$> any) *> rest) (const ""))

  let binding ?key value =
    let open Encore.Syntax in
    let value =
      value <$> (while1 is_not_lf <* (Encore.Bij.char '\x0a' <$> any))
    in
    match key with
    | Some key -> const key <* (Encore.Bij.char ' ' <$> any) <*> value
    | None -> while1 is_not_sp <* (Encore.Bij.char ' ' <$> any) <*> value

  let t =
    let open Encore.Syntax in
    binding ~key:"object" hex
    <*> binding ~key:"type" kind
    <*> binding ~key:"tag" Encore.Bij.identity
    <*> option (binding ~key:"tagger" user)
    <*> rest

  let format = Encore.Syntax.map Encore.Bij.(compose obj5 tag) t
end

let format = Syntax.format

let length t =
  let string x = Int64.of_int (String.length x) in
  let ( + ) = Int64.add in
  let user_length =
    match t.tagger with
    | Some user -> string "tagger" + 1L + Git_user.length user + 1L
    | None -> 0L
  in
  string "object"
  + 1L
  + Int64.of_int (SHA1.digest_size * 2)
  + 1L
  + string "type"
  + 1L
  + string (string_of_kind t.kind)
  + 1L
  + string "tag"
  + 1L
  + string t.tag
  + 1L
  + user_length
  + match t.message with None -> 0L | Some str -> 1L + string str

let digest value =
  Git_digest.digest Git_digest.sha1 SHA1.empty `Tag length
    (Encore.to_lavoisier format)
    value

let obj {obj; _} = obj
let tag {tag; _} = tag
let message {message; _} = message
let kind {kind; _} = kind
let tagger {tagger; _} = tagger
let equal = ( = )
let compare = Stdlib.compare
let hash = Hashtbl.hash

module Set = Set.Make (struct
  type nonrec t = t

  let compare = compare
end)

module Map = Map.Make (struct
  type nonrec t = t

  let compare = compare
end)
