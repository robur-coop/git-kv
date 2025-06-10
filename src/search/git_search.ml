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
open Lwt.Infix

module Log = struct
  let src =
    Logs.Src.create "git.search" ~doc:"logs git's internal search computation"

  include (val Logs.src_log src : Logs.LOG)
end

type pred =
  [ `Commit of SHA1.t
  | `Tag of string * SHA1.t
  | `Tree of string * SHA1.t * Git_store.Tree.perm
  | `Tree_root of SHA1.t ]

let pred t h =
  let tag t = `Tag (Git_store.Tag.tag t, Git_store.Tag.obj t) in
  Log.debug (fun l -> l ~header:"predecessor" "Read the object: %a." SHA1.pp h);
  match Git_store.read_exn t h with
  | Git_store.Object.Blob _ -> Lwt.return []
  | Git_store.Object.Commit c -> begin
    Git_store.is_shallowed t h >|= function
    | true -> [`Tree_root (Git_store.Commit.tree c)]
    | false ->
      `Tree_root (Git_store.Commit.tree c)
      :: List.map (fun x -> `Commit x) (Git_store.Commit.parents c)
  end
  | Git_store.Object.Tag t -> Lwt.return [tag t]
  | Git_store.Object.Tree t ->
    let lst =
      List.map
        (fun {Git_store.Tree.name; node; perm} -> `Tree (name, node, perm))
        (Git_store.Tree.to_list t)
    in
    Lwt.return lst

type path = [ `Tag of string * path | `Commit of path | `Path of string list ]

(* let _find_commit = List.find_map (function `Commit x -> Some x | _ -> None) *)
let find_tree_root : pred list -> SHA1.t option =
  List.find_map (function `Tree_root x -> Some x | _ -> None)

let find_tag l =
  List.find_map (function
    | `Tag (s, x) -> if l = s then Some x else None
    | _ -> None)

let find_tree l elts =
  List.find_map (function
    | `Tree (s, x, perm) -> if s = l then Some (x, perm) else None
    | _ -> None)
    elts

let rec find t hash path =
  match path with
  | `Path [] ->
    (* TODO(reynir): I think we can synthesize [`Dir] here. Is this intuition correct?! *)
    Lwt.return (Some (`Dir, hash))
  | `Tag (l, p) -> (
    pred t hash >>= fun preds ->
    match find_tag l preds with
    | None -> Lwt.return_none
    | Some s -> (find [@tailcall]) t s p)
  | `Commit p -> (
    pred t hash >>= fun preds ->
    match find_tree_root preds with
    | None -> Lwt.return_none
    | Some s -> (find [@tailcall]) t s p)
  | `Path (h :: p) -> (
    pred t hash >>= fun preds ->
    match find_tree h preds, p with
    | None, _ -> Lwt.return_none
    | Some (s, _), _ :: _ -> (find [@tailcall]) t s (`Path p)
    | Some (s, perm), [] -> Lwt.return (Some (perm, s)))

(* XXX: can do one less look-up *)
let mem t h path =
  find t h path >>= function
  | None -> Lwt.return false
  | Some _ -> Lwt.return true
