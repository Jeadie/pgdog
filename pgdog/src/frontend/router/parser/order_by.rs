//! Sorting columns extracted from the query.

use std::fmt::Debug;

use crate::net::messages::Vector;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OrderBy {
    Index(OrderByIdx),
    Column(OrderByColumn),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OrderByIdx {
    Asc(usize),
    Desc(usize),
    AscVectorL2(usize, Vector),
}

impl OrderByIdx {
    pub(crate) fn index(&self) -> usize {
        match self {
            OrderByIdx::Asc(column)
            | OrderByIdx::Desc(column)
            | OrderByIdx::AscVectorL2(column, _) => *column - 1,
        }
    }
    pub(crate) fn asc(&self) -> bool {
        matches!(self, OrderByIdx::Asc(_) | OrderByIdx::AscVectorL2(_, _))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OrderByColumn {
    Asc(String),
    Desc(String),
    AscVectorL2(String, Vector),
}

impl OrderByColumn {
    pub fn name(&self) -> &String {
        match self {
            Self::Asc(name) | Self::Desc(name) | Self::AscVectorL2(name, _) => name,
        }
    }
}

impl OrderBy {
    pub fn column(col: String, asc: bool) -> Self {
        if asc {
            Self::Column(OrderByColumn::Asc(col))
        } else {
            Self::Column(OrderByColumn::Desc(col))
        }
    }
    /// ORDER BY x ASC
    pub(crate) fn asc(&self) -> bool {
        matches!(
            self,
            OrderBy::Index(OrderByIdx::Asc(_))
                | OrderBy::Index(OrderByIdx::AscVectorL2(_, _))
                | OrderBy::Column(OrderByColumn::Asc(_))
                | OrderBy::Column(OrderByColumn::AscVectorL2(_, _))
        )
    }

    /// Column index.
    pub(crate) fn index(&self) -> Option<usize> {
        match self {
            OrderBy::Index(order_by) => Some(order_by.index()),
            OrderBy::Column(_) => None,
        }
    }

    /// ORDER BY clause contains a vector.
    pub(crate) fn vector(&self) -> Option<(&Vector, &String)> {
        match self {
            OrderBy::Column(OrderByColumn::AscVectorL2(name, vector)) => Some((vector, name)),
            _ => None,
        }
    }
}
