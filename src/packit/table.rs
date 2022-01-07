use std::any::Any;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow2::array::{
    Array, MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, TryPush,
};
use arrow2::datatypes::DataType;
use arrow2::types::NativeType;

#[derive(Copy, Clone)]
pub enum Kind {
    Bool,
    I32,
    I64,
    F64,
    String,
}

impl Kind {
    fn array_with_capacity(self, capacity: usize) -> VarArray {
        match self {
            Kind::Bool => VarArray::new(MutableBooleanArray::with_capacity(capacity)),
            Kind::I32 => VarArray::new(MutablePrimitiveArray::<i32>::with_capacity(capacity)),
            Kind::I64 => VarArray::new(MutablePrimitiveArray::<i64>::with_capacity(capacity)),
            Kind::F64 => VarArray::new(MutablePrimitiveArray::<f64>::with_capacity(capacity)),
            Kind::String => VarArray::new(MutableUtf8Array::<i32>::with_capacity(capacity)),
        }
    }

    pub fn to_arrow(self) -> DataType {
        match self {
            Kind::Bool => DataType::Boolean,
            Kind::I32 => DataType::Int32,
            Kind::I64 => DataType::Int64,
            Kind::F64 => DataType::Float64,
            Kind::String => DataType::Utf8,
        }
    }
}

struct VarArray {
    inner: Box<dyn MutableArray>,
}

impl VarArray {
    fn new<T: MutableArray + 'static>(array: T) -> Self {
        Self {
            inner: Box::new(array),
        }
    }

    fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.inner.as_mut_any().downcast_mut()
    }

    // this moves, but has to be called from a mut ref?!
    fn as_arc(&mut self) -> Arc<dyn Array> {
        self.inner.as_arc()
    }
}

pub struct Table {
    schema: Box<[Kind]>,
    builders: Box<[VarArray]>,
    cap: usize,
    mem_used: usize,
}

fn make_builders(schema: &[Kind], cap: usize) -> Box<[VarArray]> {
    schema
        .iter()
        .map(|kind| kind.array_with_capacity(cap))
        .collect()
}

impl Table {
    pub fn with_capacity(schema: &[Kind], cap: usize) -> Self {
        Self {
            schema: schema.to_vec().into_boxed_slice(),
            builders: make_builders(schema, cap),
            cap,
            mem_used: 0,
        }
    }

    pub fn mem_estimate(&self) -> usize {
        self.mem_used
    }

    pub fn rows(&self) -> usize {
        self.builders[0].inner.len()
    }

    pub fn push_null(&mut self, i: usize) -> Result<()> {
        // only off by a factor of about eight
        self.mem_used += 1;
        self.builders[i].inner.push_null();
        Ok(())
    }

    pub fn push_str(&mut self, i: usize, val: Option<&str>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableUtf8Array<i32>>() {
            self.mem_used +=
                val.map(|val| val.len()).unwrap_or_default() + std::mem::size_of::<i32>();
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!("can't push a string to this column"))
        }
    }

    pub fn push_bool(&mut self, i: usize, val: bool) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableBooleanArray>() {
            // only off by a factor of about four
            self.mem_used += 1;
            arr.try_push(Some(val))?;
            Ok(())
        } else {
            Err(anyhow!("can't push a bool to this column"))
        }
    }

    pub fn push_primitive<T: NativeType>(&mut self, i: usize, val: T) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutablePrimitiveArray<T>>() {
            self.mem_used += std::mem::size_of::<T>();
            arr.try_push(Some(val))?;
            Ok(())
        } else {
            Err(anyhow!(
                "can't push an {} to this column",
                std::any::type_name::<T>()
            ))
        }
    }

    pub fn finish_row(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn take_batch(&mut self) -> Vec<Arc<dyn Array>> {
        let ret = self.builders.iter_mut().map(|arr| arr.as_arc()).collect();
        self.builders = make_builders(&self.schema, self.cap);
        self.mem_used = 0;
        ret
    }
}
