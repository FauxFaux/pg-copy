use itertools::Itertools;

fn generate_ranges(ids: &[i64], end: i64) -> Vec<(i64, i64)> {
    let ids_per_bucket = 32;

    let buckets = ids.len() / ids_per_bucket;

    if buckets <= 1 {
        return vec![(0, end)];
    }

    let middles = (1..buckets)
        .flat_map(|bucket| ids.get(bucket * ids_per_bucket).copied())
        .dedup()
        .collect_vec();

    let mut wheres = Vec::with_capacity(middles.len() + 2);
    wheres.push((0, middles[0]));
    wheres.extend(middles.iter().copied().tuple_windows::<(i64, i64)>());
    wheres.push((middles[middles.len() - 1], end));

    wheres
}

pub fn generate_wheres(ids: &[i64], end: i64) -> Vec<String> {
    generate_ranges(ids, end)
        .into_iter()
        .map(|(left, right)| format!("id > {} AND id <= {}", left, right))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::generate_ranges;
    use anyhow::Result;
    use itertools::Itertools;

    #[test]
    fn ranges_too_few_ids() -> Result<()> {
        for i in 0..64 {
            let ids = &(0..i).into_iter().collect_vec();
            assert_eq!(vec![(0, 555)], generate_ranges(ids, 555), "0..{}", i);
        }

        for i in 64..(64 + 32) {
            let ids = &(0..i).into_iter().collect_vec();
            assert_eq!(
                vec![(0, 32), (32, 555)],
                generate_ranges(ids, 555),
                "0..{}",
                i
            );
        }
        Ok(())
    }
}
