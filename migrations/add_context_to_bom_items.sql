-- Migration: Add context column to bom_items table
-- This column stores usage context (refdes, notes, placement) as JSONB

-- Check if column already exists before adding
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'bom_items' 
        AND column_name = 'context'
    ) THEN
        ALTER TABLE bom_items 
        ADD COLUMN context JSONB DEFAULT '{}'::jsonb;
        
        -- Add comment for documentation
        COMMENT ON COLUMN bom_items.context IS 
            'Usage context for this part in the assembly (refdes, notes, placement, etc.)';
    END IF;
END $$;


