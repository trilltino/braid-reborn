// Shared by the oracle binary (upstream engine) and braid-core's differential test
// (copied engine). The includer defines `dt` as the engine crate/module under test.
// Scripts carry no document positions: each engine resolves them against its own view
// of the document at the operation's parents, so disagreement about any past state
// shows up as different text rather than being papered over by the generator.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Script {
    pub agents: usize,
    pub ops: Vec<ScriptOp>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptOp {
    pub agent: usize,
    /// Indices of earlier operations; always a frontier (no member is an ancestor of another).
    pub parents: Vec<usize>,
    /// `Some(text)` inserts; `None` deletes.
    pub insert: Option<String>,
    /// Where, in thousandths of the document length at the parents.
    pub position_permille: u32,
    /// Deletion length in thousandths of the remaining length (at least one scalar).
    pub length_permille: u32,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub text: String,
    pub operations: usize,
    pub encoded: Vec<u8>,
}

pub fn build(script: &Script) -> Result<dt::list::ListOpLog, String> {
    let mut log = dt::list::ListOpLog::new();
    let agents: Vec<_> = (0..script.agents)
        .map(|index| log.get_or_create_agent_id(&format!("agent-{index}")))
        .collect();
    let mut last = Vec::with_capacity(script.ops.len());
    for (index, op) in script.ops.iter().enumerate() {
        let mut parents = Vec::new();
        for parent in &op.parents {
            parents.push(*last.get(*parent).ok_or_else(|| format!("op {index}: forward parent {parent}"))?);
        }
        parents.sort_unstable();
        let length = log.checkout(&parents).len();
        let position = ((op.position_permille as usize) * (length + 1) / 1000).min(length);
        let agent = *agents.get(op.agent).ok_or("agent out of range")?;
        let lv = match &op.insert {
            Some(text) if !text.is_empty() => log.add_insert_at(agent, &parents, position, text),
            _ if length == 0 => log.add_insert_at(agent, &parents, 0, "."),
            _ => {
                let start = position.min(length - 1);
                let width = (((op.length_permille as usize) * (length - start) / 1000).max(1)).min(length - start);
                log.add_delete_at(agent, &parents, start..start + width)
            }
        };
        last.push(lv);
    }
    Ok(log)
}
pub fn text_of(log: &dt::list::ListOpLog) -> String {
    log.checkout_tip().content().to_string()
}
pub fn run(script: &Script) -> Result<Outcome, String> {
    let log = build(script)?;
    Ok(Outcome {
        text: text_of(&log),
        operations: log.len(),
        encoded: log.encode(&dt::list::encoding::ENCODE_FULL),
    })
}
pub fn decode_text(bytes: &[u8]) -> Result<String, String> {
    let mut log = dt::list::ListOpLog::new();
    log.decode_and_add(bytes).map_err(|error| format!("{error:?}"))?;
    Ok(text_of(&log))
}
/// Merge several encodings in the order given into one fresh log.
pub fn merge_text(chunks: &[Vec<u8>]) -> Result<String, String> {
    let mut log = dt::list::ListOpLog::new();
    for chunk in chunks {
        log.decode_and_add(chunk).map_err(|error| format!("{error:?}"))?;
    }
    Ok(text_of(&log))
}
