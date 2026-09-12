use crate::{Error, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// An update is either a leaf replacement or a patch within an existing
/// object. The wire has no type tags: objects contain operations, other JSON
/// values replace leaves. Changing between an object and a leaf requires
/// Delete followed by Create.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, try_from = "serde_json::Value")]
pub enum Update {
    Object(Patch),
    Value(Value),
}

impl TryFrom<serde_json::Value> for Update {
    type Error = Error;
    fn try_from(value: serde_json::Value) -> Result<Self, Error> {
        match value {
            serde_json::Value::Object(fields) => Ok(Self::Object(Patch::try_from(
                fields.into_iter().collect::<BTreeMap<_, _>>(),
            )?)),
            value => Ok(Self::Value(Value::from(value))),
        }
    }
}

/// Object edits with explicit creation of every property. Create accepts
/// leaves or empty objects only. Delete removes keys and their subtrees.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "BTreeMap<String, serde_json::Value>",
    rename_all = "PascalCase"
)]
pub struct Patch {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub create: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub update: BTreeMap<String, Update>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub delete: Vec<String>,
}

impl TryFrom<BTreeMap<String, serde_json::Value>> for Patch {
    type Error = Error;
    fn try_from(mut fields: BTreeMap<String, serde_json::Value>) -> Result<Self, Error> {
        fn take<T: serde::de::DeserializeOwned + Default>(
            fields: &mut BTreeMap<String, serde_json::Value>,
            key: &str,
        ) -> Result<T, Error> {
            match fields.remove(key) {
                None => Ok(T::default()),
                Some(value) => serde_json::from_value(value)
                    .map_err(|e| Error::InvalidPatch(format!("{key}: {e}"))),
            }
        }
        for key in fields.keys() {
            if !matches!(key.as_str(), "Create" | "Update" | "Delete") {
                return Err(Error::InvalidPatch(format!("unknown operation {key:?}")));
            }
        }
        let patch = Self {
            create: take(&mut fields, "Create")?,
            update: take(&mut fields, "Update")?,
            delete: take(&mut fields, "Delete")?,
        };
        patch.validate()?;
        Ok(patch)
    }
}

impl Patch {
    pub fn is_empty(&self) -> bool {
        self.create.is_empty() && self.update.is_empty() && self.delete.is_empty()
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut keys = BTreeSet::new();
        for key in self
            .create
            .keys()
            .chain(self.update.keys())
            .chain(self.delete.iter())
        {
            if !keys.insert(key) {
                return Err(Error::InvalidPatch(format!(
                    "multiple operations for {key:?}"
                )));
            }
        }
        for (key, value) in &self.create {
            if value.as_object().is_some_and(|object| !object.is_empty()) {
                return Err(Error::InvalidPatch(format!(
                    "Create {key:?}: create an empty object, then Create each child"
                )));
            }
        }
        for (key, update) in &self.update {
            match update {
                Update::Object(patch) => patch.validate()?,
                Update::Value(value) if value.as_object().is_some() => {
                    return Err(Error::InvalidPatch(format!(
                        "Update {key:?}: objects require nested operations"
                    )));
                }
                Update::Value(_) => {}
            }
        }
        Ok(())
    }

    /// Combines independent edits, recursively merging object updates.
    /// Overlapping leaf or structural operations return their conflicting path.
    pub fn merge(&self, other: &Self) -> Result<Self, Error> {
        self.validate()?;
        other.validate()?;
        self.merge_at(other, &mut Vec::new())
    }

    fn merge_at(&self, other: &Self, path: &mut Vec<String>) -> Result<Self, Error> {
        if self.is_empty() {
            return Ok(other.clone());
        }
        if other.is_empty() {
            return Ok(self.clone());
        }
        let keys: BTreeSet<_> = self
            .create
            .keys()
            .chain(self.update.keys())
            .chain(self.delete.iter())
            .collect();
        let mut merged = self.clone();
        merged.create.extend(other.create.clone());
        merged.update.extend(other.update.clone());
        merged.delete.extend(other.delete.iter().cloned());
        merged.delete.sort();
        for key in other
            .create
            .keys()
            .chain(other.update.keys())
            .chain(other.delete.iter())
        {
            if !keys.contains(key) {
                continue;
            }
            path.push(key.clone());
            let (Some(Update::Object(a)), Some(Update::Object(b))) =
                (self.update.get(key), other.update.get(key))
            else {
                return Err(Error::MergeConflict { path: path.clone() });
            };
            merged
                .update
                .insert(key.clone(), Update::Object(a.merge_at(b, path)?));
            path.pop();
        }
        Ok(merged)
    }

    /// Computes a candidate root without mutating the input, even on failure.
    /// No operation recreates a missing ancestor.
    pub fn apply(&self, value: &Value) -> Result<Value, Error> {
        self.validate()?;
        self.apply_at(value, &mut Vec::new())
    }

    fn apply_at(&self, value: &Value, path: &mut Vec<String>) -> Result<Value, Error> {
        let failure = |reason: &str, path: &Vec<String>| Error::Precondition {
            path: path.clone(),
            reason: reason.into(),
        };
        let original = value
            .as_object()
            .ok_or_else(|| failure("expected an object", path))?;
        let mut map = original.clone();
        for (key, child) in &self.create {
            path.push(key.clone());
            if map.get(key).is_some() {
                return Err(failure("Create requires an absent key", path));
            }
            map = map.insert(key.clone(), child.clone());
            path.pop();
        }
        for (key, update) in &self.update {
            path.push(key.clone());
            let current = map.get(key).ok_or_else(|| {
                failure("Update requires an existing key; use Create first", path)
            })?;
            let next = match update {
                Update::Object(patch) => patch.apply_at(current, path)?,
                Update::Value(child) => {
                    if current.as_object().is_some() {
                        return Err(failure(
                            "cannot replace an object; Delete it before Create",
                            path,
                        ));
                    }
                    if child == current {
                        current.clone()
                    } else {
                        child.clone()
                    }
                }
            };
            if !next.ptr_eq(current) {
                map = map.insert(key.clone(), next);
            }
            path.pop();
        }
        for key in &self.delete {
            path.push(key.clone());
            if map.get(key).is_none() {
                return Err(failure("Delete requires an existing key", path));
            }
            map = map.remove(key);
            path.pop();
        }
        Ok(if map.ptr_eq(original) {
            value.clone()
        } else {
            Value::object(map)
        })
    }
}
