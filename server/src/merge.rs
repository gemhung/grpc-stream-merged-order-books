use crate::orderbook::Level;

pub fn merge_greater(v: &[&[Level]]) -> Vec<Level> {
    merge_by_key(v, &mut |level| std::cmp::Reverse(level.price.clone()))
}

pub fn merge_less(v: &[&[Level]]) -> Vec<Level> {
    merge_by_key(v, &mut |level| level.price.clone())
}

// generic merge k sorted vectors
fn merge_by_key<T: Clone, F, K>(v: &[&[T]], f: &mut F) -> Vec<T>
where
    F: FnMut(&T) -> K,
    K: PartialOrd,
{
    if v.is_empty() {
        return vec![];
    }

    if v.len() == 1 {
        return v[0].to_vec();
    }

    // divide and conquer
    let len = v.len();
    let left = merge_by_key(&v[..len / 2], f);
    let right = merge_by_key(&v[len / 2..], f);
    let mut iter1 = left.iter().peekable();
    let mut iter2 = right.iter().peekable();

    let mut ret = Vec::with_capacity(left.len() + right.len());
    loop {
        match (iter1.peek(), iter2.peek()) {
            (Some(p1), Some(p2)) if f(p1).lt(&f(p2)) => ret.push(iter1.next().unwrap().clone()), // safe to unwrap cause it's a Some
            (Some(_), Some(_)) => ret.push(iter2.next().unwrap().clone()),
            // one of them has finished iterating all values
            _ => break,
        }
    }

    ret.extend(iter1.cloned());
    ret.extend(iter2.cloned());

    ret
}
