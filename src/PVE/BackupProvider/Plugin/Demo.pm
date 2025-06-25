package PVE::BackupProvider::Plugin::Demo;

use strict;
use warnings;

use File::Path qw(make_path);
use File::Copy;
use POSIX qw(strftime);

use base qw(PVE::BackupProvider::Plugin::Base);

sub new {
    my ($class, $storage_plugin, $scfg, $storeid, $log_function) = @_;

    my $self = bless {
        storage_plugin => $storage_plugin,
        scfg => $scfg,
        storeid => $storeid,
        log_function => $log_function,
    }, $class;

    $self->{scfg}->{path} //= '/tmp/demo';

    # store archives inside the storage's backup directory so that the
    # associated storage plugin can list them correctly
    $self->{backup_path} = $storage_plugin->get_subdir($self->{scfg}, 'backup');
    make_path($self->{backup_path});

    return $self;
}

sub _log {
    my ($self, $msg) = @_;
    my $ts = strftime('%Y-%m-%d %H:%M:%S', localtime());
    if (open(my $fh, '>>', '/tmp/demo.log')) {
        print $fh "[$ts] $msg\n";
        close($fh);
    }
    if (my $log = $self->{log_function}) {
        eval { $log->('info', $msg); };
    }
}

sub provider_name {
    return 'DemoProvider';
}

sub job_init {
    my ($self, $start_time) = @_;
    $self->_log("job_init at $start_time");
    return {};
}

sub job_cleanup {
    my ($self) = @_;
    $self->_log('job_cleanup');
    return {};
}

sub backup_init {
    my ($self, $vmid, $vmtype, $start_time) = @_;
    my $suffix = $vmtype eq 'lxc' ? '.tar' : '';
    my $name = "demo-$vmtype-$vmid-$start_time$suffix";
    $self->{current_archive} = $name;
    $self->{vmtype} = $vmtype;
    $self->{start_time} = $start_time;
    $self->_log("backup_init $name for VMID $vmid");
    return { 'archive-name' => $name };
}

sub _vm_archive_files {
    my ($self) = @_;
    my $base = "$self->{backup_path}/$self->{current_archive}";
    my @files;
    if ($self->{vmtype} eq 'lxc') {
        push @files, "$base";
    } else {
        push @files, glob("$base-*.raw"), "$base.conf";
    }
    return @files;
}

sub backup_cleanup {
    my ($self, $vmid, $vmtype, $success, $info) = @_;
    my $size = 0;
    $size += (stat($_))[7] // 0 for $self->_vm_archive_files();
    $self->_log("backup_cleanup size=$size success=$success");
    return { stats => { 'archive-size' => $size } };
}

sub backup_get_mechanism {
    my ($self, $vmid, $vmtype) = @_;
    return $vmtype eq 'lxc' ? 'directory' : 'file-handle';
}

sub backup_handle_log_file {
    my ($self, $vmid, $filename) = @_;
    my $dest = "$self->{backup_path}/$self->{current_archive}.log";
    File::Copy::copy($filename, $dest);
    $self->_log("stored log $dest");
    return {};
}

sub backup_vm_query_incremental {
    return;
}

sub backup_vm {
    my ($self, $vmid, $guest_config, $volumes, $info) = @_;

    my $base = "$self->{backup_path}/$self->{current_archive}";
    if (defined $guest_config) {
        my $cfgpath = "$base.conf";
        open(my $fh, '>', $cfgpath) or die "unable to write $cfgpath: $!";
        print $fh $guest_config;
        close($fh);
    }

    foreach my $dev (sort keys %$volumes) {
        my $fh = $volumes->{$dev}->{'file-handle'}
            or die "missing file handle for $dev";
        my $dest = "$base-$dev.raw";
        open(my $out, '>', $dest) or die "unable to create $dest: $!";
        my $buf;
        while (my $count = sysread($fh, $buf, 8192)) {
            die "read failed: $!" if !defined $count;
            last if $count == 0;
            my $written = syswrite($out, $buf, $count);
            die "write failed: $!" if !defined $written || $written != $count;
        }
        close($out);
        $self->_log("written VM disk $dev to $dest");
    }

    return {};
}

sub backup_container_prepare {
    my ($self, $vmid, $guest_config, $exclude, $info) = @_;
    $self->_log("backup_container_prepare for $vmid");
    return {};
}

sub backup_container {
    my ($self, $vmid, $guest_config, $exclude, $info) = @_;

    my $src = $info->{directory} or die "missing directory path";
    my $archive = "$self->{backup_path}/$self->{current_archive}";

    my $tmpconf = "$self->{backup_path}/config.$$";
    if (defined $guest_config) {
        open(my $fh, '>', $tmpconf) or die "unable to write temp config $tmpconf: $!";
        print $fh $guest_config;
        close($fh);
    }

    system('tar', 'cf', $archive, '-C', $src, '.') == 0 or die "tar failed";

    if (-f $tmpconf) {
        system('tar', 'rf', $archive, '-C', $self->{backup_path}, 'config.$$') == 0 or die "tar add failed";
        unlink $tmpconf;
    }

    $self->_log("container data archived at $archive");
    return {};
}

sub restore_get_mechanism {
    my ($self, $volname) = @_;
    my $path = $self->{storage_plugin}->filesystem_path($self->{scfg}, $volname);
    $self->_log("restore_get_mechanism for $volname -> $path");

    if ($volname =~ /\.tar$/) {
        $self->{restore_archive} = $path;
        $self->{restore_vmtype} = 'lxc';
        return ('tar', 'lxc');
    } else {
        $self->{restore_base} = $path;
        $self->{restore_vmtype} = 'qemu';
        return ('qemu-img', 'qemu');
    }
}

sub archive_get_guest_config {
    my ($self, $volname, $storeid) = @_;

    my $cfg = '';
    if ($self->{restore_vmtype} eq 'lxc') {
        my $path = $self->{restore_archive};
        if (-f $path) {
            open(my $fh, '-|', 'tar', '-O', '-xf', $path, 'config');
            local $/; $cfg = <$fh>; close($fh);
        }
    } else {
        my $cfgpath = "$self->{restore_base}.conf";
        if (-f $cfgpath) {
            open(my $fh, '<', $cfgpath) or die "unable to read $cfgpath: $!";
            local $/; $cfg = <$fh>; close($fh);
        }
    }

    $self->_log("archive_get_guest_config read length=" . length($cfg));
    return $cfg;
}

sub archive_get_firewall_config { return undef; }

sub restore_vm_init {
    my ($self, $volname) = @_;
    $self->_log("restore_vm_init $volname");
    return {};
}

sub restore_vm_cleanup {
    my ($self, $volname) = @_;
    $self->_log("restore_vm_cleanup $volname");
    return {};
}

sub restore_vm_volume_init {
    my ($self, $volname, $device_name, $info) = @_;
    my $path = "$self->{restore_base}-$device_name.raw";
    $self->_log("restore_vm_volume_init $device_name -> $path");
    return { 'qemu-img-path' => $path };
}

sub restore_vm_volume_cleanup {
    my ($self, $volname, $device_name, $info) = @_;
    $self->_log("restore_vm_volume_cleanup $device_name");
    return {};
}

sub restore_container_init {
    my ($self, $volname, $info) = @_;
    $self->_log("restore_container_init $volname");
    return { 'tar-path' => $self->{restore_archive} };
}

sub restore_container_cleanup {
    my ($self, $volname, $info) = @_;
    $self->_log("restore_container_cleanup $volname");
    return {};
}

1;
