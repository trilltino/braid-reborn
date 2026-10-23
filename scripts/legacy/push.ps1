$crates = @("braid-blob", "braid-common", "braid-core", "braid-http", "braidfs-daemon", "braidfs-nfs", "server")
foreach ($crate in $crates) {
    $p = "C:\Users\isich\309_braid_reborn\$crate"
    if (Test-Path "$p\.git") {
        Remove-Item -Recurse -Force "$p\.git"
    }
    Set-Location $p
    git init
    git add .
    git commit -m "Initial commit"
    $r = if ($crate -eq "server") { "braid-local-link-server" } else { $crate }
    gh repo create "trilltino/$r" --public --source . --remote origin --push
}
