
AC_INIT([Gauche-pg],[1.10],[mmaruska@gmail.com])
AC_CANONICAL_TARGET
AC_GAUCHE_INIT_EXT

# Sets the default value of INSTALL_TYPE macro.
AC_GAUCHE_INSTALL_TYPE(sys)

# Get compiler parameters which Gauche has been compiled with.
AC_GAUCHE_CC
AC_GAUCHE_FLAGS

# Check for other programs.
AC_PROG_INSTALL

# Add more test

# Set LDFLAGS to generate shared library.
AC_GAUCHE_FIX_LIBS

# Output
AC_GAUCHE_EXT_FIXUP(pg)
AM_INIT_AUTOMAKE
AC_CONFIG_FILES([Makefile])
AC_OUTPUT

