CREATE TABLE `threads` (
	`id` text PRIMARY KEY NOT NULL,
	`title` text NOT NULL,
	`model` text NOT NULL,
	`status` text DEFAULT 'active' NOT NULL,
	`seeded_from` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `turns` (
	`id` text PRIMARY KEY NOT NULL,
	`thread_id` text NOT NULL,
	`input` text NOT NULL,
	`final_response` text DEFAULT '' NOT NULL,
	`status` text NOT NULL,
	`usage_json` text,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE no action
);
