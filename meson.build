project('pipewire-module-ariacast', 'c',
  version : '1.0.0',
  default_options : [
    'c_std=gnu11',
    'warning_level=2',
    'optimization=2',
  ])

cc = meson.get_compiler('c')

pipewire_dep = dependency('libpipewire-0.3', version : '>= 0.3.40')
thread_dep   = dependency('threads')
dbus_dep     = dependency('dbus-1', required : true)
m_dep        = cc.find_library('m', required : false)

# Ask pkg-config where PipeWire expects its modules.
# PipeWire 1.x ships as libpipewire-0.3 but may install modules under
# a versioned path like  /usr/lib/.../pipewire-1.0  or pipewire-0.3;
# moduledir from pkg-config is always correct regardless of version.
pw_module_dir = pipewire_dep.get_variable(
  pkgconfig    : 'moduledir',
  default_value: get_option('libdir') / 'pipewire-0.3')

shared_module('pipewire-module-ariacast',
  'module-ariacast.c',
  dependencies : [pipewire_dep, thread_dep, dbus_dep, m_dep],
  # Default name_prefix 'lib' produces libpipewire-module-ariacast.so
  install      : true,
  install_dir  : pw_module_dir)

# Config snippet: installed as an example only. Installing it into an
# active pipewire.conf.d/ directory would load the module for every
# user – and a second time for users who also copy it to
#   ~/.config/pipewire/pipewire.conf.d/ariacast.conf
conf_dir = get_option('datadir') / 'doc' / meson.project_name()
install_data('pipewire.conf.d/ariacast.conf',
  install_dir : conf_dir)

summary({
  'PipeWire version'  : pipewire_dep.version(),
  'Module install dir': pw_module_dir,
  'Example config dir': get_option('prefix') / conf_dir,
}, section : 'Build')
