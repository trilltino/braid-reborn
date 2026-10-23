# Historical repository scripts

These files are retained as historical tooling for the old
`C:\Users\isich\309_braid_reborn` checkout and its separate crate repositories.
Their paths, package lists and generated documentation do not describe this
workspace.

| Script | Original purpose |
| --- | --- |
| `generate_readmes.py` | Overwrite package and module READMEs from static templates |
| `write_gitignores.py` | Generate ignore files for separate repositories |
| `push_crates.py` | Rewrite path dependencies and create/publish GitHub repositories |
| `push.ps1` | Remove nested Git metadata and create/publish GitHub repositories |
| `push_readmes.ps1` | Commit and push README changes to those repositories |

Use the root Cargo workspace and [current scripts](../README.md) for development.
Publishing a workspace release needs a separate, reviewed release procedure.
