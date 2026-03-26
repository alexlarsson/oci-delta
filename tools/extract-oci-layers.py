#!/usr/bin/env python3
"""
Extract layers from an OCI archive, optionally filtering by path prefix.
Useful for extracting ostree objects for local bootc-delta testing.
"""

import argparse
import gzip
import json
import os
import sys
import tarfile
from pathlib import Path


def extract_oci_layers(oci_archive, output_dir, prefix=None, strip_prefix=False, verbose=False):
    """Extract layers from OCI archive, optionally filtering by prefix."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Opening OCI archive: {oci_archive}")

    # Open the OCI archive
    with tarfile.open(oci_archive, 'r') as oci_tar:
        # Read index.json
        try:
            index_member = oci_tar.getmember('index.json')
            index_data = oci_tar.extractfile(index_member).read()
            index = json.loads(index_data)
        except KeyError:
            print("Error: index.json not found in OCI archive", file=sys.stderr)
            return 1

        if verbose:
            print(f"Found {len(index.get('manifests', []))} manifest(s) in index")

        if not index.get('manifests'):
            print("Error: No manifests in index.json", file=sys.stderr)
            return 1

        # Get the first manifest
        manifest_digest = index['manifests'][0]['digest']
        manifest_algo, manifest_hash = manifest_digest.split(':', 1)
        manifest_path = f'blobs/{manifest_algo}/{manifest_hash}'

        if verbose:
            print(f"Reading manifest: {manifest_digest[:16]}...")

        try:
            manifest_member = oci_tar.getmember(manifest_path)
            manifest_data = oci_tar.extractfile(manifest_member).read()
            manifest = json.loads(manifest_data)
        except KeyError:
            print(f"Error: Manifest {manifest_path} not found", file=sys.stderr)
            return 1

        layers = manifest.get('layers', [])
        if verbose:
            print(f"Found {len(layers)} layer(s) in manifest")

        # Extract each layer
        files_extracted = 0
        for i, layer in enumerate(layers):
            layer_digest = layer['digest']
            layer_algo, layer_hash = layer_digest.split(':', 1)
            layer_path = f'blobs/{layer_algo}/{layer_hash}'

            if verbose:
                print(f"\nProcessing layer {i+1}/{len(layers)}: {layer_digest[:16]}...")

            try:
                layer_member = oci_tar.getmember(layer_path)
                layer_data = oci_tar.extractfile(layer_member)
            except KeyError:
                print(f"Warning: Layer {layer_path} not found, skipping", file=sys.stderr)
                continue

            # Layer is gzip-compressed tar
            try:
                with gzip.open(layer_data, 'rb') as gz:
                    with tarfile.open(fileobj=gz, mode='r') as layer_tar:
                        members = layer_tar.getmembers()

                        # Filter by prefix if specified
                        if prefix:
                            filtered_members = [
                                m for m in members
                                if m.name.startswith(prefix) or m.name.startswith('./' + prefix)
                            ]
                            if verbose:
                                print(f"  Filtered {len(members)} files to {len(filtered_members)} matching prefix '{prefix}'")
                            members = filtered_members

                        # Extract files
                        for member in members:
                            # Remove prefix from extraction path if strip_prefix is specified
                            if prefix and strip_prefix:
                                # Strip the prefix from the path
                                name = member.name
                                if name.startswith('./'):
                                    name = name[2:]
                                if name.startswith(prefix):
                                    name = name[len(prefix):]
                                    if name.startswith('/'):
                                        name = name[1:]

                                    # Update member name for extraction
                                    member.name = name

                            if member.isfile() or member.isdir():
                                try:
                                    layer_tar.extract(member, output_path)
                                    if member.isfile():
                                        files_extracted += 1
                                except Exception as e:
                                    print(f"Warning: Failed to extract {member.name}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"Warning: Failed to process layer {layer_digest[:16]}: {e}", file=sys.stderr)
                continue

    if verbose:
        print(f"\nExtracted {files_extracted} file(s) to {output_dir}")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Extract layers from an OCI archive, optionally filtering by path prefix.',
        epilog='''Example usage:
  # Extract ostree objects for bootc-delta apply (preserves full path structure):
  %(prog)s image.oci-archive /tmp/delta-source --prefix sysroot/ostree/repo/objects/
  bootc-delta apply --delta-source /tmp/delta-source update.delta output.oci-archive

  # Extract and strip prefix:
  %(prog)s image.oci-archive /tmp/objects --prefix sysroot/ostree/repo/objects/ --strip-prefix''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('oci_archive', help='Path to OCI archive file')
    parser.add_argument('output_dir', help='Output directory for extracted files')
    parser.add_argument('--prefix', help='Only extract files matching this prefix')
    parser.add_argument('--strip-prefix', action='store_true', help='Strip the prefix from extracted file paths (default: preserve full paths)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if not os.path.exists(args.oci_archive):
        print(f"Error: OCI archive not found: {args.oci_archive}", file=sys.stderr)
        return 1

    return extract_oci_layers(args.oci_archive, args.output_dir, args.prefix, args.strip_prefix, args.verbose)


if __name__ == '__main__':
    sys.exit(main())
