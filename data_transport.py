"""
This is a function to transport csv generated in 
"""
import os, base64, json, requests

GITHUB_TOKEN   = os.environ.get("community-data-access")  #Generated token in targeted generation folder
OWNER          = "sltan0331"                    #Owner of github
REPO           = "community-data"                # Objective repository
BRANCH         = "main"
LOCAL_DIR      = "out"                            # Objective folder name in data-pipeline
TARGET_PREFIX  = "data/states/commodities/2020"   # Objective folder name in targeted generation folder

session = requests.Session()
session.headers.update({
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
})

def get_file_sha(owner, repo, path, ref):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    r = session.get(url)
    if r.status_code == 200:
        return r.json()["sha"]  # sha is needed to update objective when the objective already exists
    elif r.status_code == 404:
        return None
    else:
        raise RuntimeError(f"GET contents failed: {r.status_code} {r.text}")

def put_file(owner, repo, path, content_bytes, message, branch, sha=None):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    data = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": branch
    }
    if sha:
        data["sha"] = sha
    r = session.put(url, data=json.dumps(data))
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT contents failed: {r.status_code} {r.text}")

def upload_dir(local_dir, target_prefix):
    for root, _, files in os.walk(local_dir):
        for fname in files:
            # Only transport csv
            if not fname.lower().endswith(".csv"):
                continue
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
            target_path = f"{target_prefix}/{rel_path}"
            with open(local_path, "rb") as f:
                data = f.read()
            sha = get_file_sha(OWNER, REPO, target_path, BRANCH)
            put_file(
                OWNER, REPO, target_path, data,
                message=f"update {target_path} from data-pipeline",
                branch=BRANCH, sha=sha
            )
            print(f"[OK] uploaded: {target_path}")

if __name__ == "__main__":
    if not GITHUB_TOKEN:
        raise SystemExit("Set env GITHUB_TOKEN=your_pat_with_repo_write")
    upload_dir(LOCAL_DIR, TARGET_PREFIX)
    print("All done.")
