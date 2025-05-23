module SHA1 = Digestif.SHA1

type t =
  | Blob of Git_blob.t
  | Commit of Git_commit.t
  | Tree of Git_tree.t
  | Tag of Git_tag.t

let digest = function
  | Commit c -> (Git_commit.digest c : SHA1.t)
  | Tree t -> Git_tree.digest t
  | Blob b -> Git_blob.digest b
  | Tag t -> Git_tag.digest t

let to_bstr = function
  | Blob v -> v
  | Commit v ->
    let chunk = Int64.to_int (Git_commit.length v) in
    Encore.Lavoisier.emit_string ~chunk v
      (Encore.to_lavoisier Git_commit.format)
    |> Bstr.of_string
  | Tag v ->
    let chunk = Int64.to_int (Git_tag.length v) in
    Encore.Lavoisier.emit_string ~chunk v (Encore.to_lavoisier Git_tag.format)
    |> Bstr.of_string
  | Tree v ->
    let chunk = Int64.to_int (Git_tree.length v) in
    Encore.Lavoisier.emit_string ~chunk v (Encore.to_lavoisier Git_tree.format)
    |> Bstr.of_string

type with_parser = [ `Commit | `Tree | `Tag ]

let of_bstr ~kind bstr =
  match kind with
  | `Blob -> Ok (Blob bstr)
  | #with_parser as kind -> (
    let parser =
      let open Angstrom in
      match kind with
      | `Commit -> Encore.to_angstrom Git_commit.format >>| fun v -> Commit v
      | `Tag -> Encore.to_angstrom Git_tag.format >>| fun v -> Tag v
      | `Tree -> Encore.to_angstrom Git_tree.format >>| fun v -> Tree v
    in
    let res =
      Angstrom.parse_bigstring ~consume:Angstrom.Consume.All parser bstr
    in
    match res with Ok v -> Ok v | Error _ -> Error (`Msg "Invalid Git object"))

let length = function
  | Commit c -> Git_commit.length c
  | Tree t -> Git_tree.length t
  | Blob b -> Git_blob.length b
  | Tag t -> Git_tag.length t
