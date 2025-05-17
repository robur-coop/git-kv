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

module SHA1 = struct
  include Digestif.SHA1

  module Set = Set.Make (struct
    type nonrec t = t

    let compare = unsafe_compare
  end)
end

let src = Logs.Src.create "git.traverse" ~doc:"logs git's traverse event"

module Log = (val Logs.src_log src : Logs.LOG)

(* XXX(dinosaure): convenience and common part between the file-system and
   the mem back-end - to avoid redundant code. *)

module type S = sig
  type t

  val read_exn : t -> SHA1.t -> Git_object.t
  val root : t -> Fpath.t
  val is_shallowed : t -> SHA1.t -> bool Lwt.t
end

module Make (Store : S) = struct
  let fold
      t
      (fn :
        'acc ->
        ?name:Fpath.t ->
        length:int64 ->
        SHA1.t ->
        Git_object.t ->
        'acc Lwt.t)
      ~path
      acc
      hash =
    let names = Hashtbl.create 0x100 in
    let open Lwt.Infix in
    let rec walk close rest queue acc =
      match rest with
      | [] -> (
        match Queue.pop queue with
        | rest -> walk close [rest] queue acc
        | exception Queue.Empty -> Lwt.return acc)
      | hash :: rest -> (
        if SHA1.Set.mem hash close then walk close rest queue acc
        else
          let close' = SHA1.Set.add hash close in
          match Store.read_exn t hash with
          | Git_object.Commit commit as value -> begin
            let rest' = Git_commit.tree commit :: rest in
            Store.is_shallowed t hash >>= function
            | true ->
              fn acc ~length:(Git_commit.length commit) hash value
              >>= fun acc' -> walk close' rest' queue acc'
            | false ->
              List.iter (fun x -> Queue.add x queue) (Git_commit.parents commit);
              fn acc ~length:(Git_commit.length commit) hash value
              >>= fun acc' -> walk close' rest' queue acc'
          end
          | Git_object.Tree tree as value ->
            let path = try Hashtbl.find names hash with Not_found -> path in
            Lwt_list.iter_s
              (fun {Git_tree.name; node; _} ->
                Hashtbl.add names node Fpath.(path / name);
                Lwt.return ())
              (Git_tree.to_list tree)
            >>= fun () ->
            let rest' =
              rest
              @ List.map
                  (fun {Git_tree.node; _} -> node)
                  (Git_tree.to_list tree)
            in
            fn acc ~name:path ~length:(Git_tree.length tree) hash value
            >>= fun acc' -> walk close' rest' queue acc'
          | Git_object.Blob blob as value ->
            let path = try Hashtbl.find names hash with Not_found -> path in
            fn acc ~name:path ~length:(Git_blob.length blob) hash value
            >>= fun acc' -> walk close' rest queue acc'
          | Git_object.Tag tag as value ->
            Queue.add (Git_tag.obj tag) queue;
            fn acc ~length:(Git_tag.length tag) hash value >>= fun acc' ->
            walk close' rest queue acc')
    in
    walk SHA1.Set.empty [hash] (Queue.create ()) acc

  let iter t fn hash =
    fold t
      (fun () ?name:_ ~length:_ hash value -> fn hash value)
      ~path:(Store.root t) () hash
end
