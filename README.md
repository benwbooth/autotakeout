# autotakeout

One `uv` script for the Google Takeout email flow, with repo-local Nix and Python dependency files.

The script needs one Gmail API OAuth client JSON so it can monitor your inbox for the Takeout email. This is not your Google password or a Takeout archive. It is the downloaded Google Cloud file usually named like `client_secret_...apps.googleusercontent.com.json`.

Gmail setup:

1. Open https://console.cloud.google.com/apis/library/gmail.googleapis.com
2. Select or create a Google Cloud project.
3. Click `Enable` for the Gmail API.
4. Open https://console.cloud.google.com/auth/overview
5. If Google Auth Platform is not configured, click `Get started`.
6. App name: `autotakeout`; user support email: your email.
7. Audience: choose `Internal` if this is a Workspace account and it is offered; otherwise choose `External`/`Testing` and add your own Gmail as a test user if asked.
8. Contact email: your email. Accept the user data policy and finish.
9. Open https://console.cloud.google.com/auth/clients
10. Click `Create client`, choose application type `Desktop app`.
11. Name it `autotakeout`, click `Create`, then download the JSON file.
12. Put that JSON in `~/Downloads` or `~/.config/autotakeout`. Do not commit it.

Then run:

```sh
nix develop
./autotakeout.py
```

If you use direnv, run `direnv allow` once and skip `nix develop` after that.

The script searches for `client_secret*.json` in `~/Downloads` and `~/.config/autotakeout`; auto-detects Brave/Chrome/Chromium; defaults to Google Photos, Gmail/Mail, Google Drive, `data/raw`, and `data/merged`; then saves those preferences to `~/.config/autotakeout/config.json`.

It checks Gmail auth, checks browser login, opens Takeout if an export is not ready yet, waits on Gmail, downloads the archive links, extracts each completed `.tgz` into one merged directory while the next archive is downloading, and backs up the merged output with restic. The raw `.tgz` directory is not backed up by default.

The accepted product aliases are `photos`, `drive`, `gmail`, and `mail`. The selected products are saved in the config file for future runs. To export only Photos:

```sh
./autotakeout.py --products photos
```

The default Gmail search only considers Takeout emails from the last 8 days because Google says Takeout archives expire in about 7 days. Override with `--query` if you need a different window.

The main flow is designed to be rerunnable. It records a pending Takeout export so reruns wait for the email instead of creating duplicate exports, skips already verified archive downloads, deletes orphaned corrupt partial archives before retrying, merges extraction output without duplicating existing files, and runs restic with `--skip-if-unchanged`.

If Google says `This browser or app may not be secure`, the script was using an automation-controlled browser. Current versions launch normal Brave/Chrome directly for login. Rerun `./autotakeout.py --force-login`; if the dedicated profile is wedged, remove `~/.local/state/autotakeout/browser-profile` and rerun.

If Google asks for your password again before archive downloads, the script prompts for it in the terminal only when needed and never stores it. You can also pass it once with `--google-password`.

To create or use a Backblaze B2 bucket and initialize restic automatically:

```sh
./autotakeout.py \
  --b2-bucket globally-unique-bucket-name
```

For B2 credentials, set either `B2_ACCOUNT_ID`/`B2_ACCOUNT_KEY` or `B2_APPLICATION_KEY_ID`/`B2_APPLICATION_KEY`. If they are not set, the script prompts for them. It stores the B2 application key in `~/.local/state/autotakeout/secrets.json` with `0600` permissions so future runs can proceed unattended. It creates `~/.local/state/autotakeout/restic-password` if no `RESTIC_PASSWORD_FILE` is set. Keep a separate copy of that restic password file; without it, the backup cannot be restored.

Restic is part of the normal flow. Use `--no-restic` only when you explicitly want to skip backup. After backup, the script writes a compact validation manifest with file counts, total bytes, and SHA-256 hashes for a few sample files. It verifies restic by restoring that manifest, comparing counts and sizes, restoring and hashing the sample files, and running `restic check --read-data-subset 1%`. Use `--restic-sample-count` and `--restic-sample-max-mib` to tune the sample, or `--restic-full-check` to run `restic check --read-data` instead.

To verify an existing configured restic backup without touching Gmail or Takeout:

```sh
./autotakeout.py verify
```

To list snapshots in the configured restic repo:

```sh
./autotakeout.py snapshots
```

By default this lists only `autotakeout`-tagged snapshots. Use `--all` to show every snapshot in the repo, `--latest N` to limit the output, or `--json` for restic's raw snapshot JSON.

To browse the latest restic snapshot in your file manager:

```sh
./autotakeout.py mount
```

This FUSE-mounts the configured restic repo under `~/.local/state/autotakeout/restic-mount`, opens the selected snapshot's merged Takeout directory with `xdg-open` on Linux or `open` on macOS, and keeps the mount alive until you press `Ctrl-C`. To browse an older snapshot, pass its snapshot ID or unique prefix:

```sh
./autotakeout.py mount c8fe8077
```

To open Backrest against the configured restic repo:

```sh
./autotakeout.py backrest
```

This writes an autotakeout-specific Backrest config under `~/.local/state/autotakeout/backrest/`, checks that Docker is running, starts `garethgeorge/backrest:v1.13.0` at `http://127.0.0.1:9898`, asks Backrest to index the restic snapshots, and opens it in your browser. Press `Ctrl-C` in the terminal to stop the Backrest container. If the browser does not open automatically, visit that URL manually. The backup plan points at the merged Takeout directory; schedules are disabled so Backrest will not start its own backup while this script is already uploading.

To back up and verify already-downloaded/already-extracted files without touching Gmail or Takeout:

```sh
./autotakeout.py backup --b2-bucket globally-unique-bucket-name
```

Debug escape hatches:

```sh
./autotakeout.py login --credentials ~/Downloads/client_secret_*.json --browser "$(command -v brave)"
./autotakeout.py links --credentials ~/Downloads/client_secret_*.json --show
./autotakeout.py download --browser "$(command -v brave)" --raw data/raw
./autotakeout.py extract --raw data/raw --merged data/merged
```

## License

MIT
