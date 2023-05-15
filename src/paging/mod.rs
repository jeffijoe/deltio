/// Represents a pagination query.
pub struct Paging {
    /// How big the page may be.
    size: usize,

    /// The pagination offset, ie where the next page starts.
    offset: Option<usize>,
}

impl Paging {
    /// Creates a new `Paging`.
    pub fn new(size: usize, offset: Option<usize>) -> Self {
        Self {
            offset,
            // Ensure that passing 0 as a page size results in a default.
            size: match size {
                0 => 20,
                v if v > 1_000 => 1_000,
                _ => size,
            },
        }
    }

    /// Creates a new `Paging` with no offset.
    pub fn start(size: usize) -> Self {
        Self { size, offset: None }
    }

    /// Returns the capped paging size limit.
    pub fn size(&self) -> usize {
        // Cap the page size.
        self.size.min(10_000)
    }

    /// Returns the page offset.
    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    /// Returns the value to pass into [`Iterator::skip`].
    pub fn to_skip(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    /// Creates a new `Paging` for the next page.
    pub fn next_page(&self, new_offset: Option<usize>) -> Self {
        Self {
            size: self.size,
            offset: new_offset,
        }
    }

    /// Given a result where this paging was applied, returns new paging
    /// parameters that will yield the next page.
    ///
    /// If the [`result`] was empty, then the next [`offset`] will be [`None`].
    pub fn next_page_from_slice_result<T>(&self, result: &[T]) -> Self {
        let skip_value = self.to_skip();
        // If we got at least one element, then we want to return a new offset.
        let new_offset = if !result.is_empty() {
            Some(skip_value + result.len())
        } else {
            None
        };

        self.next_page(new_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_page_from_slice_result_returns_next_offset() {
        let paging = Paging::start(5);
        let result = (1..=12).collect::<Vec<usize>>();

        let next = paging.next_page_from_slice_result(&result[..5]);
        assert_eq!(next.offset(), Some(5));

        let next = next.next_page_from_slice_result(&result[5..10]);
        assert_eq!(next.offset(), Some(10));

        let next = next.next_page_from_slice_result(&result[10..12]);
        assert_eq!(next.offset(), Some(12));

        let eos = next.next_page_from_slice_result::<usize>(&[]);
        assert!(eos.offset().is_none());
    }
}
