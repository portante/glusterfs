############################################################################################################
# Command to build rpms.#
# $ rpmbuild -ta %{name}-%{version}-%{release}.tar.gz #
############################################################################################################
# Setting up the environment. #
#  * Create a directory %{name}-%{version} under $HOME/rpmbuild/SOURCES #
#  * Copy the contents of plugins directory into $HOME/rpmbuild/SOURCES/%{name}-%{version} #
#  * tar zcvf %{name}-%{version}-%{release}.tar.gz $HOME/rpmbuild/SOURCES/%{name}-%{version} %{name}.spec #
# For more information refer #
# http://fedoraproject.org/wiki/How_to_create_an_RPM_package #
############################################################################################################
%if ! (0%{?fedora} > 12 || 0%{?rhel} > 5)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%endif

%define _confdir     /etc/swift
%define _swiftdir    /usr/lib/python2.6/site-packages/swift
%define _ufo_version 1.0
%define _ufo_release 5.pdq.2%{?dist}

Summary  : GlusterFS Unified File and Object Storage.
Name     : gluster-swift-plugin
Version  : %{_ufo_version}
Release  : %{_ufo_release}
Group    : Application/File
Vendor   : Red Hat Inc.
Source0  : %{name}-%{version}-%{release}.tar.gz
Packager : gluster-users@gluster.org
License  : Apache
BuildArch: noarch
Requires : memcached
Requires : openssl
Requires : python
Requires : gluster-swift

%description
Gluster Unified File and Object Storage unifies NAS and object storage
technology. This provides a system for data storage that enables users to access
the same data as an object and as a file, simplifying management and controlling
storage costs.

%prep
%setup -q

%install
rm -rf %{buildroot}

mkdir -p %{buildroot}/%{_swiftdir}/plugins

cp constraints.py  %{buildroot}/%{_swiftdir}/plugins
cp DiskDir.py      %{buildroot}/%{_swiftdir}/plugins
cp DiskFile.py     %{buildroot}/%{_swiftdir}/plugins
cp Glusterfs.py    %{buildroot}/%{_swiftdir}/plugins
cp __init__.py     %{buildroot}/%{_swiftdir}/plugins
cp utils.py        %{buildroot}/%{_swiftdir}/plugins

mkdir -p %{buildroot}/%{_confdir}/

cp -r conf/*       %{buildroot}/%{_confdir}/

%files
%defattr(-,root,root)
%{_swiftdir}/plugins
%{_confdir}/*.builder
%{_confdir}/*.ring.gz
%{_confdir}/db_file.db
%config %{_confdir}/account-server.conf-gluster
%config %{_confdir}/container-server.conf-gluster
%config %{_confdir}/object-server.conf-gluster
%config %{_confdir}/swift.conf-gluster
%config %{_confdir}/proxy-server.conf-gluster
%config %{_confdir}/fs.conf-gluster
