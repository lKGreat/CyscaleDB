namespace CyscaleDB.Core.Common;

/// <summary>
/// Manages MySQL system variables for session and global scope.
/// </summary>
public sealed class SystemVariables
{
    private readonly Dictionary<string, object?> _sessionVariables;
    private static readonly Dictionary<string, object?> _globalVariables;

    /// <summary>
    /// Default system variable values (MySQL compatible).
    /// </summary>
    private static readonly Dictionary<string, object?> DefaultVariables = new(StringComparer.OrdinalIgnoreCase)
    {
        // Server information
        ["version"] = Constants.ServerVersion,
        ["version_comment"] = "CyscaleDB Server",
        ["version_compile_os"] = Environment.OSVersion.Platform.ToString(),
        ["version_compile_machine"] = Environment.Is64BitOperatingSystem ? "x86_64" : "x86",

        // Character set and collation
        ["character_set_client"] = "utf8mb4",
        ["character_set_connection"] = "utf8mb4",
        ["character_set_database"] = "utf8mb4",
        ["character_set_results"] = "utf8mb4",
        ["character_set_server"] = "utf8mb4",
        ["character_set_system"] = "utf8mb4",
        ["collation_connection"] = "utf8mb4_general_ci",
        ["collation_database"] = "utf8mb4_general_ci",
        ["collation_server"] = "utf8mb4_general_ci",

        // Connection settings
        ["autocommit"] = 1,
        ["auto_increment_increment"] = 1,
        ["auto_increment_offset"] = 1,
        ["connect_timeout"] = 10,
        ["interactive_timeout"] = 28800,
        ["wait_timeout"] = 28800,
        ["net_read_timeout"] = 30,
        ["net_write_timeout"] = 60,

        // SQL mode
        ["sql_mode"] = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",

        // Transaction
        ["transaction_isolation"] = "REPEATABLE-READ",
        ["tx_isolation"] = "REPEATABLE-READ",
        ["transaction_read_only"] = 0,
        ["tx_read_only"] = 0,

        // General settings
        ["max_allowed_packet"] = 67108864, // 64MB
        ["max_connections"] = 151,
        ["lower_case_table_names"] = 0,
        ["time_zone"] = "SYSTEM",
        ["system_time_zone"] = TimeZoneInfo.Local.StandardName,

        // Performance
        ["query_cache_size"] = 0,
        ["query_cache_type"] = "OFF",

        // Features
        ["have_ssl"] = "DISABLED",
        ["have_openssl"] = "DISABLED",
        ["have_query_cache"] = "NO",
        ["have_compress"] = "YES",
        ["have_dynamic_loading"] = "YES",
        ["have_geometry"] = "YES",
        ["have_profiling"] = "YES",
        ["have_statement_timeout"] = "YES",
        ["have_symlink"] = "DISABLED",

        // Protocol
        ["protocol_version"] = 10,
        ["hostname"] = Environment.MachineName,
        ["port"] = Constants.DefaultPort,

        // Compatibility
        ["sql_auto_is_null"] = 0,
        ["sql_big_selects"] = 1,
        ["sql_buffer_result"] = 0,
        ["sql_log_bin"] = 0,
        ["sql_notes"] = 1,
        ["sql_quote_show_create"] = 1,
        ["sql_safe_updates"] = 0,
        ["sql_select_limit"] = 18446744073709551615UL,
        ["sql_warnings"] = 0,

        // License
        ["license"] = "MIT",

        // ══════════════════════════════════════════════
        // InnoDB variables (~100)
        // ══════════════════════════════════════════════
        ["innodb_buffer_pool_size"] = 134217728L,  // 128MB
        ["innodb_buffer_pool_instances"] = 1,
        ["innodb_buffer_pool_chunk_size"] = 134217728L,
        ["innodb_buffer_pool_dump_at_shutdown"] = 1,
        ["innodb_buffer_pool_dump_pct"] = 25,
        ["innodb_buffer_pool_load_at_startup"] = 1,
        ["innodb_log_file_size"] = 50331648L, // 48MB
        ["innodb_log_files_in_group"] = 2,
        ["innodb_log_buffer_size"] = 16777216, // 16MB
        ["innodb_flush_log_at_trx_commit"] = 1,
        ["innodb_flush_method"] = "fsync",
        ["innodb_file_per_table"] = 1,
        ["innodb_data_file_path"] = "ibdata1:12M:autoextend",
        ["innodb_data_home_dir"] = "",
        ["innodb_doublewrite"] = 1,
        ["innodb_doublewrite_pages"] = 4,
        ["innodb_io_capacity"] = 200,
        ["innodb_io_capacity_max"] = 2000,
        ["innodb_read_io_threads"] = 4,
        ["innodb_write_io_threads"] = 4,
        ["innodb_thread_concurrency"] = 0,
        ["innodb_concurrency_tickets"] = 5000,
        ["innodb_commit_concurrency"] = 0,
        ["innodb_lock_wait_timeout"] = 50,
        ["innodb_deadlock_detect"] = 1,
        ["innodb_print_all_deadlocks"] = 0,
        ["innodb_table_locks"] = 1,
        ["innodb_max_dirty_pages_pct"] = 90.0,
        ["innodb_max_dirty_pages_pct_lwm"] = 10.0,
        ["innodb_adaptive_hash_index"] = 1,
        ["innodb_adaptive_hash_index_parts"] = 8,
        ["innodb_adaptive_flushing"] = 1,
        ["innodb_adaptive_flushing_lwm"] = 10,
        ["innodb_change_buffering"] = "all",
        ["innodb_change_buffer_max_size"] = 25,
        ["innodb_old_blocks_pct"] = 37,
        ["innodb_old_blocks_time"] = 1000,
        ["innodb_open_files"] = 4096,
        ["innodb_page_size"] = 16384,
        ["innodb_purge_threads"] = 4,
        ["innodb_purge_batch_size"] = 300,
        ["innodb_rollback_on_timeout"] = 0,
        ["innodb_stats_auto_recalc"] = 1,
        ["innodb_stats_persistent"] = 1,
        ["innodb_stats_persistent_sample_pages"] = 20,
        ["innodb_stats_transient_sample_pages"] = 8,
        ["innodb_stats_on_metadata"] = 0,
        ["innodb_sort_buffer_size"] = 1048576, // 1MB
        ["innodb_spin_wait_delay"] = 6,
        ["innodb_sync_spin_loops"] = 30,
        ["innodb_strict_mode"] = 1,
        ["innodb_autoinc_lock_mode"] = 2,
        ["innodb_checksum_algorithm"] = "crc32",
        ["innodb_compression_failure_threshold_pct"] = 5,
        ["innodb_compression_level"] = 6,
        ["innodb_compression_pad_pct_max"] = 50,
        ["innodb_default_row_format"] = "dynamic",
        ["innodb_fill_factor"] = 100,
        ["innodb_ft_enable_stopword"] = 1,
        ["innodb_ft_max_token_size"] = 84,
        ["innodb_ft_min_token_size"] = 3,
        ["innodb_ft_num_word_optimize"] = 2000,
        ["innodb_ft_result_cache_limit"] = 2000000000L,
        ["innodb_ft_sort_pll_degree"] = 2,
        ["innodb_ft_total_cache_size"] = 640000000L,
        ["innodb_ft_cache_size"] = 8000000,
        ["innodb_online_alter_log_max_size"] = 134217728L,
        ["innodb_optimize_fulltext_only"] = 0,
        ["innodb_redo_log_capacity"] = 104857600L,
        ["innodb_undo_directory"] = "",
        ["innodb_undo_tablespaces"] = 2,
        ["innodb_max_undo_log_size"] = 1073741824L,
        ["innodb_undo_log_truncate"] = 1,
        ["innodb_status_output"] = 0,
        ["innodb_status_output_locks"] = 0,
        ["innodb_monitor_enable"] = "",
        ["innodb_monitor_disable"] = "",
        ["innodb_monitor_reset"] = "",
        ["innodb_monitor_reset_all"] = "",
        ["innodb_print_ddl_log"] = 0,
        ["innodb_temp_data_file_path"] = "ibtmp1:12M:autoextend",
        ["innodb_tmpdir"] = "",
        ["innodb_use_native_aio"] = 1,
        ["innodb_validate_tablespace_paths"] = 1,
        ["innodb_version"] = "8.4.0",

        // ══════════════════════════════════════════════
        // Performance & Buffer variables (~60)
        // ══════════════════════════════════════════════
        ["sort_buffer_size"] = 262144, // 256KB
        ["join_buffer_size"] = 262144,
        ["read_buffer_size"] = 131072, // 128KB
        ["read_rnd_buffer_size"] = 262144,
        ["bulk_insert_buffer_size"] = 8388608, // 8MB
        ["tmp_table_size"] = 16777216, // 16MB
        ["max_heap_table_size"] = 16777216,
        ["key_buffer_size"] = 8388608,
        ["myisam_sort_buffer_size"] = 8388608,
        ["myisam_max_sort_file_size"] = 9223372036853727232L,
        ["myisam_repair_threads"] = 1,
        ["preload_buffer_size"] = 32768,
        ["max_length_for_sort_data"] = 4096,
        ["max_sort_length"] = 1024,
        ["max_join_size"] = 18446744073709551615UL,
        ["optimizer_switch"] = "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on",
        ["optimizer_prune_level"] = 1,
        ["optimizer_search_depth"] = 62,
        ["optimizer_trace"] = "enabled=off,one_line=off",
        ["optimizer_trace_max_mem_size"] = 1048576,
        ["eq_range_index_dive_limit"] = 200,
        ["range_optimizer_max_mem_size"] = 8388608,
        ["thread_cache_size"] = 9,
        ["thread_stack"] = 1048576,
        ["table_open_cache"] = 4000,
        ["table_definition_cache"] = 2000,
        ["table_open_cache_instances"] = 16,
        ["open_files_limit"] = 65535,
        ["binlog_cache_size"] = 32768,
        ["binlog_stmt_cache_size"] = 32768,
        ["max_binlog_cache_size"] = 18446744073709547520UL,
        ["max_binlog_stmt_cache_size"] = 18446744073709547520UL,

        // ══════════════════════════════════════════════
        // Security & SSL variables (~30)
        // ══════════════════════════════════════════════
        ["require_secure_transport"] = 0,
        ["ssl_ca"] = "",
        ["ssl_capath"] = "",
        ["ssl_cert"] = "",
        ["ssl_cipher"] = "",
        ["ssl_crl"] = "",
        ["ssl_crlpath"] = "",
        ["ssl_key"] = "",
        ["tls_version"] = "TLSv1.2,TLSv1.3",
        ["tls_ciphersuites"] = "",
        ["admin_ssl_ca"] = "",
        ["admin_ssl_cert"] = "",
        ["admin_ssl_key"] = "",
        ["admin_tls_version"] = "TLSv1.2,TLSv1.3",
        ["password_history"] = 0,
        ["password_reuse_interval"] = 0,
        ["password_require_current"] = 0,
        ["default_password_lifetime"] = 0,
        ["authentication_policy"] = "*,,",
        ["caching_sha2_password_auto_generate_rsa_keys"] = 1,
        ["caching_sha2_password_private_key_path"] = "private_key.pem",
        ["caching_sha2_password_public_key_path"] = "public_key.pem",
        ["sha256_password_auto_generate_rsa_keys"] = 1,
        ["sha256_password_private_key_path"] = "private_key.pem",
        ["sha256_password_public_key_path"] = "public_key.pem",
        ["mandatory_roles"] = "",
        ["activate_all_roles_on_login"] = 0,

        // ══════════════════════════════════════════════
        // Logging variables (~25)
        // ══════════════════════════════════════════════
        ["general_log"] = 0,
        ["general_log_file"] = "general.log",
        ["slow_query_log"] = 0,
        ["slow_query_log_file"] = "slow.log",
        ["long_query_time"] = 10.0,
        ["log_output"] = "FILE",
        ["log_error"] = "stderr",
        ["log_error_verbosity"] = 2,
        ["log_queries_not_using_indexes"] = 0,
        ["log_slow_admin_statements"] = 0,
        ["log_slow_extra"] = 0,
        ["log_throttle_queries_not_using_indexes"] = 0,
        ["log_timestamps"] = "UTC",
        ["log_bin"] = 0,
        ["log_bin_basename"] = "",
        ["log_bin_index"] = "",
        ["log_slave_updates"] = 1,
        ["log_replica_updates"] = 1,
        ["log_bin_trust_function_creators"] = 0,
        ["binlog_format"] = "ROW",
        ["binlog_row_image"] = "FULL",
        ["binlog_rows_query_log_events"] = 0,
        ["binlog_expire_logs_seconds"] = 2592000,
        ["binlog_gtid_simple_recovery"] = 1,
        ["binlog_order_commits"] = 1,
        ["sync_binlog"] = 1,

        // ══════════════════════════════════════════════
        // Replication variables (~50)
        // ══════════════════════════════════════════════
        ["server_id"] = 1,
        ["server_uuid"] = Guid.NewGuid().ToString(),
        ["gtid_mode"] = "OFF",
        ["enforce_gtid_consistency"] = "OFF",
        ["gtid_executed"] = "",
        ["gtid_purged"] = "",
        ["gtid_owned"] = "",
        ["gtid_executed_compression_period"] = 0,
        ["replica_parallel_workers"] = 4,
        ["replica_parallel_type"] = "LOGICAL_CLOCK",
        ["replica_preserve_commit_order"] = 1,
        ["replica_net_timeout"] = 60,
        ["replica_skip_errors"] = "OFF",
        ["replica_type_conversions"] = "",
        ["relay_log"] = "",
        ["relay_log_basename"] = "",
        ["relay_log_index"] = "",
        ["relay_log_info_repository"] = "TABLE",
        ["relay_log_purge"] = 1,
        ["relay_log_recovery"] = 0,
        ["relay_log_space_limit"] = 0L,
        ["master_info_repository"] = "TABLE",
        ["source_verify_checksum"] = 0,
        ["replica_compressed_protocol"] = 0,
        ["replica_exec_mode"] = "STRICT",
        ["replica_load_tmpdir"] = "",
        ["replica_max_allowed_packet"] = 1073741824,
        ["replica_pending_jobs_size_max"] = 134217728,
        ["replica_sql_verify_checksum"] = 1,
        ["replica_transaction_retries"] = 10,

        // ══════════════════════════════════════════════
        // Session variables (~50)
        // ══════════════════════════════════════════════
        ["group_concat_max_len"] = 1024L,
        ["div_precision_increment"] = 4,
        ["explicit_defaults_for_timestamp"] = 1,
        ["foreign_key_checks"] = 1,
        ["unique_checks"] = 1,
        ["updatable_views_with_limit"] = "YES",
        ["default_storage_engine"] = "InnoDB",
        ["default_tmp_storage_engine"] = "InnoDB",
        ["internal_tmp_mem_storage_engine"] = "TempTable",
        ["big_tables"] = 0,
        ["max_execution_time"] = 0,
        ["max_error_count"] = 1024,
        ["max_user_connections"] = 0,
        ["max_connect_errors"] = 100,
        ["max_digest_length"] = 1024,
        ["max_prepared_stmt_count"] = 16382,
        ["max_sp_recursion_depth"] = 0,
        ["max_points_in_geometry"] = 65536,
        ["max_write_lock_count"] = 18446744073709551615UL,
        ["max_seeks_for_key"] = 18446744073709551615UL,
        ["min_examined_row_limit"] = 0,
        ["net_buffer_length"] = 16384,
        ["net_retry_count"] = 10,
        ["profiling"] = 0,
        ["profiling_history_size"] = 15,
        ["pseudo_thread_id"] = 0,
        ["rand_seed1"] = 0L,
        ["rand_seed2"] = 0L,
        ["range_alloc_block_size"] = 4096,
        ["regexp_stack_limit"] = 8000000,
        ["regexp_time_limit"] = 32,
        ["show_create_table_verbosity"] = 0,
        ["show_old_temporals"] = 0,
        ["stored_program_cache"] = 256,
        ["stored_program_definition_cache"] = 256,
        ["temptable_max_mmap"] = 1073741824L,
        ["temptable_max_ram"] = 1073741824L,
        ["temptable_use_mmap"] = 1,
        ["timestamp"] = 0,
        ["unique_checks"] = 1,
        ["windowing_use_high_precision"] = 1,
        ["cte_max_recursion_depth"] = 1000,
        ["generated_random_password_length"] = 20,
        ["histogram_generation_max_mem_size"] = 20000000,
        ["information_schema_stats_expiry"] = 86400,
        ["lc_messages"] = "en_US",
        ["lc_messages_dir"] = "",
        ["lc_time_names"] = "en_US",
        ["default_week_format"] = 0,
        ["block_encryption_mode"] = "aes-128-ecb",
        ["completion_type"] = "NO_CHAIN",
        ["concurrent_insert"] = "AUTO",
        ["connection_memory_chunk_size"] = 8912,
        ["connection_memory_limit"] = 18446744073709551615UL,
        ["global_memory_limit"] = 18446744073709551615UL,

        // ══════════════════════════════════════════════
        // Event scheduler
        // ══════════════════════════════════════════════
        ["event_scheduler"] = "OFF",

        // ══════════════════════════════════════════════
        // Performance Schema
        // ══════════════════════════════════════════════
        ["performance_schema"] = 1,
        ["performance_schema_accounts_size"] = -1,
        ["performance_schema_digests_size"] = 10000,
        ["performance_schema_events_stages_history_long_size"] = 10000,
        ["performance_schema_events_stages_history_size"] = 10,
        ["performance_schema_events_statements_history_long_size"] = 10000,
        ["performance_schema_events_statements_history_size"] = 10,
        ["performance_schema_events_transactions_history_long_size"] = 10000,
        ["performance_schema_events_transactions_history_size"] = 10,
        ["performance_schema_events_waits_history_long_size"] = 10000,
        ["performance_schema_events_waits_history_size"] = 10,
        ["performance_schema_hosts_size"] = -1,
        ["performance_schema_max_cond_classes"] = 150,
        ["performance_schema_max_cond_instances"] = -1,
        ["performance_schema_max_digest_length"] = 1024,
        ["performance_schema_max_file_classes"] = 80,
        ["performance_schema_max_file_handles"] = 32768,
        ["performance_schema_max_file_instances"] = -1,
        ["performance_schema_max_index_stat"] = -1,
        ["performance_schema_max_memory_classes"] = 450,
        ["performance_schema_max_metadata_locks"] = -1,
        ["performance_schema_max_mutex_classes"] = 350,
        ["performance_schema_max_mutex_instances"] = -1,
        ["performance_schema_max_prepared_statements_instances"] = -1,
        ["performance_schema_max_program_instances"] = -1,
        ["performance_schema_max_rwlock_classes"] = 60,
        ["performance_schema_max_rwlock_instances"] = -1,
        ["performance_schema_max_socket_classes"] = 10,
        ["performance_schema_max_socket_instances"] = -1,
        ["performance_schema_max_sql_text_length"] = 1024,
        ["performance_schema_max_stage_classes"] = 175,
        ["performance_schema_max_statement_classes"] = 218,
        ["performance_schema_max_statement_stack"] = 10,
        ["performance_schema_max_table_handles"] = -1,
        ["performance_schema_max_table_instances"] = -1,
        ["performance_schema_max_table_lock_stat"] = -1,
        ["performance_schema_max_thread_classes"] = 100,
        ["performance_schema_max_thread_instances"] = -1,
        ["performance_schema_session_connect_attrs_size"] = 512,
        ["performance_schema_setup_actors_size"] = -1,
        ["performance_schema_setup_objects_size"] = -1,
        ["performance_schema_users_size"] = -1,

        // ══════════════════════════════════════════════
        // Misc server variables
        // ══════════════════════════════════════════════
        ["basedir"] = "",
        ["datadir"] = "",
        ["tmpdir"] = "",
        ["pid_file"] = "",
        ["socket"] = "",
        ["skip_name_resolve"] = 0,
        ["skip_networking"] = 0,
        ["skip_show_database"] = 0,
        ["skip_external_locking"] = 1,
        ["character_sets_dir"] = "",
        ["init_connect"] = "",
        ["init_file"] = "",
        ["log_error_services"] = "log_filter_internal; log_sink_internal",
        ["disabled_storage_engines"] = "",
        ["default_authentication_plugin"] = "caching_sha2_password",
        ["default_collation_for_utf8mb4"] = "utf8mb4_0900_ai_ci",
        ["admin_address"] = "",
        ["admin_port"] = 33062,
        ["bind_address"] = "*",
        ["create_admin_listener_thread"] = 0,
        ["back_log"] = 151,
        ["delayed_insert_timeout"] = 300,
        ["delayed_queue_size"] = 1000,
        ["flush"] = 0,
        ["flush_time"] = 0,
        ["ft_boolean_syntax"] = "+ -><()~*:\"\"&|",
        ["ft_max_word_len"] = 84,
        ["ft_min_word_len"] = 4,
        ["ft_query_expansion_limit"] = 20,
        ["ft_stopword_file"] = "",
        ["local_infile"] = 0,
        ["lock_wait_timeout"] = 31536000,
        ["locked_in_memory"] = 0,
        ["ngram_token_size"] = 2,
        ["offline_mode"] = 0,
        ["old_alter_table"] = 0,
        ["old_passwords"] = 0,
        ["partial_revokes"] = 0,
        ["persisted_globals_load"] = 1,
        ["print_identified_with_as_hex"] = 0,
        ["read_only"] = 0,
        ["super_read_only"] = 0,
        ["secure_file_priv"] = "",
        ["select_into_buffer_size"] = 131072,
        ["select_into_disk_sync"] = 0,
        ["select_into_disk_sync_delay"] = 0,
        ["session_track_gtids"] = "OFF",
        ["session_track_schema"] = 1,
        ["session_track_state_change"] = 0,
        ["session_track_system_variables"] = "time_zone,autocommit,character_set_client,character_set_results,character_set_connection",
        ["session_track_transaction_info"] = "OFF",
    };

    static SystemVariables()
    {
        _globalVariables = new Dictionary<string, object?>(DefaultVariables, StringComparer.OrdinalIgnoreCase);
    }

    public SystemVariables()
    {
        _sessionVariables = new Dictionary<string, object?>(DefaultVariables, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets a session variable value.
    /// </summary>
    public object? GetSession(string name)
    {
        return _sessionVariables.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>
    /// Sets a session variable value.
    /// </summary>
    public void SetSession(string name, object? value)
    {
        _sessionVariables[name] = value;
    }

    /// <summary>
    /// Gets a global variable value.
    /// </summary>
    public static object? GetGlobal(string name)
    {
        return _globalVariables.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>
    /// Sets a global variable value.
    /// </summary>
    public static void SetGlobal(string name, object? value)
    {
        _globalVariables[name] = value;
    }

    /// <summary>
    /// Gets a variable value with the specified scope.
    /// </summary>
    public object? Get(string name, bool isGlobal = false)
    {
        return isGlobal ? GetGlobal(name) : GetSession(name);
    }

    /// <summary>
    /// Sets a variable value with the specified scope.
    /// </summary>
    public void Set(string name, object? value, bool isGlobal = false)
    {
        if (isGlobal)
            SetGlobal(name, value);
        else
            SetSession(name, value);
    }

    /// <summary>
    /// Gets all session variables.
    /// </summary>
    public IEnumerable<KeyValuePair<string, object?>> GetAllSession()
    {
        return _sessionVariables;
    }

    /// <summary>
    /// Gets all global variables.
    /// </summary>
    public static IEnumerable<KeyValuePair<string, object?>> GetAllGlobal()
    {
        return _globalVariables;
    }

    /// <summary>
    /// Checks if a variable name exists in session or global scope.
    /// </summary>
    public bool Exists(string name, bool isGlobal = false)
    {
        return isGlobal
            ? _globalVariables.ContainsKey(name)
            : _sessionVariables.ContainsKey(name);
    }

    /// <summary>
    /// Gets session variables matching a SQL LIKE pattern.
    /// </summary>
    public IEnumerable<KeyValuePair<string, object?>> GetSessionLike(string pattern)
    {
        var regex = LikePatternToRegex(pattern);
        return _sessionVariables.Where(kv => regex.IsMatch(kv.Key));
    }

    /// <summary>
    /// Gets global variables matching a SQL LIKE pattern.
    /// </summary>
    public static IEnumerable<KeyValuePair<string, object?>> GetGlobalLike(string pattern)
    {
        var regex = LikePatternToRegex(pattern);
        return _globalVariables.Where(kv => regex.IsMatch(kv.Key));
    }

    /// <summary>
    /// Converts a SQL LIKE pattern to a .NET Regex (case-insensitive).
    /// '%' matches any sequence; '_' matches a single character.
    /// </summary>
    private static System.Text.RegularExpressions.Regex LikePatternToRegex(string pattern)
    {
        var escaped = System.Text.RegularExpressions.Regex.Escape(pattern);
        escaped = escaped.Replace("%", ".*").Replace("_", ".");
        return new System.Text.RegularExpressions.Regex(
            $"^{escaped}$",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    /// <summary>
    /// Gets the total count of known system variables.
    /// </summary>
    public static int Count => DefaultVariables.Count;

    /// <summary>
    /// Converts a variable value to a DataValue.
    /// </summary>
    public static DataValue ToDataValue(object? value)
    {
        return value switch
        {
            null => DataValue.Null,
            string s => DataValue.FromVarChar(s),
            int i => DataValue.FromInt(i),
            long l => DataValue.FromBigInt(l),
            ulong ul => DataValue.FromBigInt((long)ul),
            bool b => DataValue.FromInt(b ? 1 : 0),
            double d => DataValue.FromDouble(d),
            _ => DataValue.FromVarChar(value.ToString() ?? "")
        };
    }
}
