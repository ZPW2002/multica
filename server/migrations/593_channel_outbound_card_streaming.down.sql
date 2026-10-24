ALTER TABLE channel_outbound_card_message
    DROP COLUMN IF EXISTS card_entity_id,
    DROP COLUMN IF EXISTS last_sequence;
