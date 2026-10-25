# Third-party notice: copied CRDT engine

`diamond_types/` and `rle/` in this directory are derived from
[diamond-types](https://github.com/josephg/diamond-types) by Seph Gentle, with
local modifications. Upstream declares the engine `license = "ISC"` and its `rle`
crate `license = "ISC OR Apache-2.0"`; its README states the code is published under
the ISC license.

The ISC license requires that the copyright notice and permission notice accompany
copies. The upstream repository snapshot examined (`89ae3a0ab8d941a2885e6ec89d094bc9ce9d2922`)
does not contain a license file or a copyright line for the engine crate, so none is
reproduced here and none has been invented. **Open action:** obtain the exact
copyright line and license text from the upstream author, then add it to this file.

The closest upstream revision that could be identified, and the size of the local
changes, are recorded in [engine provenance](../../../../docs/audit/engine-provenance.md).
That is similarity evidence only; the exact source revision and patch set of the
original copy were not recorded when it was introduced.
