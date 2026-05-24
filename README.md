# autotakeout

One self-contained `nix shell` + `uv` script for the Google Takeout email flow.

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
./autotakeout.py
```

The script searches for `client_secret*.json` in `~/Downloads` and `~/.config/autotakeout`; auto-detects Brave/Chrome/Chromium; defaults to `data/raw` and `data/merged`; then saves those preferences to `~/.config/autotakeout/config.json`.

It checks Gmail auth, checks browser login, opens Takeout if an export is not ready yet, waits on Gmail, downloads the archive links, extracts all `.tgz` files into one merged directory, and backs up the raw and merged outputs with restic.

If Google says `This browser or app may not be secure`, the script was using an automation-controlled browser. Current versions launch normal Brave/Chrome directly for login. Rerun `./autotakeout.py --force-login`; if the dedicated profile is wedged, remove `~/.local/state/autotakeout/browser-profile` and rerun.

To create or use a Backblaze B2 bucket and initialize restic automatically:

```sh
./autotakeout.py \
  --b2-bucket globally-unique-bucket-name
```

For B2 credentials, set either `B2_ACCOUNT_ID`/`B2_ACCOUNT_KEY` or `B2_APPLICATION_KEY_ID`/`B2_APPLICATION_KEY`. If they are not set, the script prompts for them. It creates `~/.local/state/autotakeout/restic-password` if no `RESTIC_PASSWORD_FILE` is set.

Restic is part of the normal flow. Use `--no-restic` only when you explicitly want to skip backup. After backup, the script writes a compact validation manifest with file counts, total bytes, and SHA-256 hashes for a few sample files. It verifies restic by restoring that manifest, comparing counts and sizes, restoring and hashing the sample files, and running `restic check --read-data-subset 1%`. Use `--restic-sample-count` and `--restic-sample-max-mib` to tune the sample, or `--restic-full-check` to run `restic check --read-data` instead.

To verify an existing configured restic backup without touching Gmail or Takeout:

```sh
./autotakeout.py verify
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
