import os
import re
import subprocess
import time

target_dir = r"C:\Users\isich\309_braid_reborn"

versions = {
    "braid-core": "0.1.4",
    "braid-http": "0.1.0",
    "braid-blob": "0.1.0",
    "braid-common": "0.1.0"
}

crates = [
    "braid-blob",
    "braid-common",
    "braid-core",
    "braid-http",
    "braidfs-daemon",
    "braidfs-nfs",
    "server"
]

for crate_dir in crates:
    full_path = os.path.join(target_dir, crate_dir)
    if not os.path.isdir(full_path):
        continue
    
    # Update Cargo.toml
    toml_path = os.path.join(full_path, "Cargo.toml")
    if os.path.exists(toml_path):
        with open(toml_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        for crate, ver in versions.items():
            content = re.sub(rf'path\s*=\s*"\.\./{crate}"', f'version = "{ver}"', content)
            content = re.sub(rf"path\s*=\s*'\.\./{crate}'", f'version = "{ver}"', content)
            
        with open(toml_path, "w", encoding="utf-8") as f:
            f.write(content)
    
    print(f"Processing {crate_dir}...")
    repo_name = f"braid-{crate_dir}" if crate_dir == "server" else crate_dir
    
    try:
        subprocess.run(["git", "init"], cwd=full_path, check=True)
        subprocess.run(["git", "add", "."], cwd=full_path, check=True)
        subprocess.run(["git", "commit", "-m", "Initial commit for standalone crate"], cwd=full_path, check=False)
        
        # We need to drop existing git remotes if they exist
        try:
            subprocess.run(["git", "remote", "remove", "origin"], cwd=full_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        print(f"Creating repo trilltino/{repo_name}...")
        res = subprocess.run([
            "gh", "repo", "create", f"trilltino/{repo_name}",
            "--public", "--source", ".", "--remote", "origin", "--push"
        ], cwd=full_path, capture_output=True, text=True)
        
        print(f"Stdout: {res.stdout}")
        print(f"Stderr: {res.stderr}")
        
        print(f"Success for {crate_dir}")
    except Exception as e:
        print(f"Error processing {crate_dir}: {e}")
    
    time.sleep(1)
