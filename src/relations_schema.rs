use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, ShortString};
use triblespace::prelude::*;

pub mod playground_relations {
    use super::*;

    attributes! {
        "8F162B593D390E1424394DBF6883A72C" as pub alias: ShortString;
        "F0AD0BBFAC4C4C899637573DC965622E" as pub first_name: Handle<Blake3, LongString>;
        "764DD765142B3F4725B614BD3B9118EC" as pub last_name: Handle<Blake3, LongString>;
        "DC0916CB5F640984EFE359A33105CA9A" as pub display_name: Handle<Blake3, LongString>;
    }

    #[allow(non_upper_case_globals)]
    pub const kind_person: Id = id_hex!("D8ADDE47121F4E7868017463EC860726");
}
