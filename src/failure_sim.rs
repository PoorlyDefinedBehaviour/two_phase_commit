//! Contains functions used to decide if an error should be simulated.

use rand::Rng;

pub fn participant_should_not_be_able_to_commit() -> bool {
    rand::thread_rng().gen_bool(0.1)
}

pub fn prepare_commit_should_fail() -> bool {
    rand::thread_rng().gen_bool(0.1)
}

pub fn commit_should_fail() -> bool {
    rand::thread_rng().gen_bool(0.1)
}

pub fn abort_should_fail() -> bool {
    rand::thread_rng().gen_bool(0.1)
}

pub fn query_transaction_state_should_fail() -> bool {
    rand::thread_rng().gen_bool(0.1)
}
