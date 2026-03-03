import os

gitignore_content = """
/target
Cargo.lock
.env
"""

target_dir = r"C:\Users\isich\309_braid_reborn"
crates = [
    "braid-blob",
    "braid-common",
    "braid-core",
    "braid-http",
    "braidfs-daemon",
    "braidfs-nfs",
    "server"
]

for crate in crates:
    full_path = os.path.join(target_dir, crate)
    gitignore_path = os.path.join(full_path, ".gitignore")
    if not os.path.exists(gitignore_path):
        with open(gitignore_path, "w") as f:
            f.write(gitignore_content)
