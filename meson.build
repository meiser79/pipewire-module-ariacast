project('pipewire-module-ariacast', 'c',
  version : '1.0.0',
  default_options : [
    'c_std=gnu11',
    'warning_level=2',
    'optimization=2',
    # sysconfdir must be absolute; /etc is correct for system installs.
    # Override with  -Dsysconfdir=/etc  if your distro differs.
    'sysconfdir=/etc',
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
  # Allow PipeWire symbols to resolve from the host process at load time.
  link_args    : ['-Wl,--allow-shlib-undefined'],
  # Default name_prefix 'lib' produces libpipewire-module-ariacast.so
  install      : true,
  install_dir  : pw_module_dir)

# User config snippet: installed to /etc/pipewire/pipewire.conf.d/
# Users should copy (or symlink) this to
#   ~/.config/pipewire/pipewire.conf.d/ariacast.conf
conf_dir = get_option('sysconfdir') / 'pipewire' / 'pipewire.conf.d'
install_data('pipewire.conf.d/ariacast.conf',
  install_dir : conf_dir)

summary({
  'PipeWire version'  : pipewire_dep.version(),
  'Module install dir': pw_module_dir,
  'Config install dir': conf_dir,
}, section : 'Build')
