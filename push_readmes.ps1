$crates = @("braid-blob", "braid-common", "braid-core", "braid-http", "braidfs-daemon", "braidfs-nfs", "server")
foreach ($crate in $crates) {
    $p = "C:\Users\isich\309_braid_reborn\$crate"
    if (Test-Path "$p\.git") {
        Set-Location $p
        git add .
        git commit -m "docs: Comprehensive READMEs covering applications, core integration, technology stack and test coverage"
        git push origin main
    }
}
