from firexapp.plugins import load_plugin_modules_from_env

# unregistering dups and marking plugins is done by firexapp.plugins._worker_init_signal() since we need to copy the
# signals, and they're available at this time
load_plugin_modules_from_env(unregister_dups_and_mark_plugins=False)