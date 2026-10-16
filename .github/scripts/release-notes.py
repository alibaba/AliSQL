#!/usr/bin/env python3
"""Render release notes from the changelog in the source being packaged."""

import argparse
import datetime
import posixpath
import re
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit


TAG_PATTERN = re.compile(r"AliSQL-(8\.0|5\.7)\.([0-9]+)-([0-9A-Za-z][0-9A-Za-z._-]*)")
LINK_PATTERN = re.compile(r"(!?\[[^\]\n]*\]\()([^\s()]+)(\))")


def parse_tag(tag):
    match = TAG_PATTERN.fullmatch(tag)
    if not match:
        raise ValueError("Expected an AliSQL-8.0.x-suffix or AliSQL-5.7.x-suffix tag")
    return tag.removeprefix("AliSQL-"), f"{match[1]}.{match[2]}"


def find_changelog(source_root, base_version):
    pattern = re.compile(
        rf"changes-in-alisql-{re.escape(base_version)}\.(\d{{4}}-\d{{2}}-\d{{2}})-en\.md"
    )
    candidates = []
    for path in (source_root / "wiki/changes").glob("*-en.md"):
        match = pattern.fullmatch(path.name)
        if match:
            candidates.append((datetime.date.fromisoformat(match[1]), path))
    if not candidates:
        if base_version == "8.0.44":
            raise ValueError("The selected source has no dated English 8.0.44 changelog")
        return None
    return max(candidates)[1]


def render_changelog(source_root, changelog, blob_base):
    relative_file = changelog.relative_to(source_root).as_posix()

    def replace_link(match):
        target = urlsplit(match[2])
        if target.scheme or target.netloc:
            return match[0]
        if target.path.startswith("/"):
            path = posixpath.normpath(target.path.lstrip("/"))
        elif target.path:
            path = posixpath.normpath(posixpath.join(posixpath.dirname(relative_file), target.path))
        else:
            path = relative_file
        if path == ".." or path.startswith("../") or not (source_root / path).exists():
            raise ValueError(f"Invalid local changelog link: {match[2]}")
        url = urlunsplit(("https", "github.com", f"{blob_base}/{quote(path)}", target.query, target.fragment))
        return f"{match[1]}{url}{match[3]}"

    lines = changelog.read_text(encoding="utf-8").splitlines()
    result = []
    fence = None
    for index, line in enumerate(lines):
        if index == 0 and line.startswith("# "):
            continue
        if fence is None and line.startswith("[ ["):
            continue
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            if fence is None:
                fence = marker[1]
            elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                fence = None
            result.append(line)
            continue
        if fence is None:
            if re.match(r"^#{1,5} ", line):
                line = "#" + line
            line = LINK_PATTERN.sub(replace_link, line)
        result.append(line)
    return "\n".join(result).strip()


def render_notes(source_root, repository, tag, docker_image, docker_repository, link_ref=None):
    source_root = Path(source_root).resolve()
    version, base_version = parse_tag(tag)
    for name in (repository, docker_image, docker_repository):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name):
            raise ValueError(f"Invalid repository/image name: {name}")
    ref = quote(link_ref or tag, safe="")
    blob_base = f"/{repository}/blob/{ref}"
    release_base = f"https://github.com/{repository}/releases/download/{tag}"
    sections = [
        f"# AliSQL {version}\n",
        "Prebuilt AliSQL binaries for Linux x86_64 and aarch64 (glibc 2.17).\n",
        "## Binary Packages\n",
        "| Platform | Package |",
        "| --- | --- |",
    ]
    for arch in ("x86_64", "aarch64"):
        package = f"alisql-{version}-linux-glibc2.17-{arch}.tar.xz"
        sections.append(f"| Linux {arch} | [{package}]({release_base}/{package}) |")
    sections += [
        "\n## Docker Image\n",
        "The linux/amd64 and linux/arm64 image is built after both binary packages are published.\n",
        f"- Image: [{docker_image}:{version}](https://hub.docker.com/r/{docker_image}/tags?name={version})",
        f"- Build status: [GitHub Actions](https://github.com/{docker_repository}/actions/workflows/build.yml)\n",
        f"```bash\ndocker pull {docker_image}:{version}\n```",
    ]
    changelog = find_changelog(source_root, base_version)
    if changelog:
        chinese = changelog.with_name(changelog.name.replace("-en.md", "-zh.md"))
        if not chinese.is_file():
            raise ValueError(f"Missing Chinese changelog: {chinese}")
        english_url = f"https://github.com{blob_base}/{changelog.relative_to(source_root).as_posix()}"
        chinese_url = f"https://github.com{blob_base}/{chinese.relative_to(source_root).as_posix()}"
        sections += [
            "\n## Changelog\n",
            f"[English changelog]({english_url}) | [Chinese changelog]({chinese_url})\n",
            render_changelog(source_root, changelog, blob_base),
        ]
    return "\n".join(sections) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--docker-image", default="songhuaxiong/alisql")
    parser.add_argument("--docker-repository", default="p1p1bear/alisql-docker")
    parser.add_argument("--link-ref", help="Use an existing commit for documentation links during validation")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        notes = render_notes(args.source_root, args.repository, args.tag,
                             args.docker_image, args.docker_repository, args.link_ref)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(notes, encoding="utf-8")
    print(f"Release notes written to {args.output}")


if __name__ == "__main__":
    main()
