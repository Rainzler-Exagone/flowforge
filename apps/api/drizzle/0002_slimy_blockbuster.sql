ALTER TABLE "jobs" ADD COLUMN "attempts" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "jobs" ADD COLUMN "max_attempts" integer DEFAULT 3 NOT NULL;