CREATE TABLE pet_activities (
	pet_id TEXT, 
	pet_name TEXT, 
	activity_type TEXT, 
	duration_minutes BIGINT, 
	timestamp TIMESTAMP, 
	staff_id TEXT, 
	notes TEXT, 
	date DATE, 
	hour INTEGER, 
	day_of_week TEXT
);
CREATE TABLE environment (
	timestamp TIMESTAMP, 
	temperature_f FLOAT, 
	humidity_percent FLOAT, 
	noise_level_db FLOAT, 
	kennel_section TEXT, 
	date DATE, 
	hour INTEGER, 
	temp_comfort TEXT, 
	humidity_comfort TEXT, 
	noise_comfort TEXT
);
CREATE TABLE staff_logs (
	staff_id TEXT, 
	staff_name TEXT, 
	shift_start TIMESTAMP, 
	shift_end TIMESTAMP, 
	section_assigned TEXT, 
	tasks_completed BIGINT, 
	notes TEXT, 
	shift_duration_hours FLOAT, 
	shift_type TEXT, 
	tasks_per_hour FLOAT
);
CREATE TABLE daily_summary (
	date DATE, 
	total_activities BIGINT, 
	total_activity_minutes BIGINT, 
	unique_pets BIGINT, 
	avg_temperature FLOAT, 
	avg_humidity FLOAT, 
	avg_noise FLOAT, 
	staff_shifts BIGINT, 
	total_tasks BIGINT
);
