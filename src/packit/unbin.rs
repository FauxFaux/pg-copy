use std::io::Read;

use anyhow::{anyhow, ensure, Context, Result};
use byteorder::{ReadBytesExt, BE};

pub struct Unbin<R> {
    inner: R,
    fields: u16,
}

pub struct Row {
    buf: Vec<u8>,
    fields: Vec<Option<(usize, usize)>>,
}

impl<R: Read> Unbin<R> {
    pub fn new(mut inner: R, fields: u16) -> Result<Unbin<R>> {
        let mut buf = [0u8; 11 + 4 + 4];
        inner.read_exact(&mut buf)?;
        ensure!(
            b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0" == &buf,
            "bad file header: {:?}",
            buf
        );
        Ok(Self { inner, fields })
    }

    pub fn next(&mut self) -> Result<Option<Row>> {
        let row_fields = self
            .inner
            .read_i16::<BE>()
            .with_context(|| anyhow!("reading row marker"))?;
        if row_fields == -1 {
            return Ok(None);
        }
        ensure!(
            Ok(self.fields) == u16::try_from(row_fields),
            "unexpected row fields: {:?}",
            row_fields
        );

        let mut fields = Vec::with_capacity(usize::from(self.fields));
        let mut buf = Vec::with_capacity(usize::from(self.fields));

        for _ in 0..self.fields {
            let len = self.inner.read_i32::<BE>()?;
            if len == -1 {
                fields.push(None);
                continue;
            }
            let len = usize::try_from(len)?;
            let old_end = buf.len();
            buf.extend((0..len).map(|_| 0));
            let new_end = buf.len();
            self.inner.read_exact(&mut buf[old_end..new_end])?;
            fields.push(Some((old_end, new_end)));
        }

        Ok(Some(Row { fields, buf }))
    }
}

impl Row {
    pub(crate) fn get(&self, i: u16) -> Option<&[u8]> {
        self.fields[usize::from(i)].map(|(l, r)| &self.buf[l..r])
    }
}
