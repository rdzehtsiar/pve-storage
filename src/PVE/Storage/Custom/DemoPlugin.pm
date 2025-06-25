package PVE::Storage::Custom::DemoPlugin;

use strict;
use warnings;

use File::Basename qw(basename);
use File::stat;

use PVE::Storage; # for APIVER constant

use base qw(PVE::Storage::DirPlugin);

sub type {
    return 'demo';
}

sub plugindata {
    return {
        content => [ { backup => 1 }, { backup => 1 } ],
        features => { 'backup-provider' => 1 },
        'sensitive-properties' => {},
    };
}

# no additional properties over DirPlugin
sub properties {
    return {};
}


# path defaults to /tmp/demo via check_config


sub options {
    return {
        path => { optional => 1 },
        'content-dirs' => { optional => 1 },
        nodes => { optional => 1 },
        shared => { optional => 1 },
        disable => { optional => 1 },
        maxfiles => { optional => 1 },
        'prune-backups' => { optional => 1 },
        'max-protected-backups' => { optional => 1 },
        content => { optional => 1 },
        format => { optional => 1 },
        mkdir => { optional => 1 },
        'create-base-path' => { optional => 1 },
        'create-subdirs' => { optional => 1 },
        is_mountpoint => { optional => 1 },
        bwlimit => { optional => 1 },
        preallocation => { optional => 1 },
    };
}

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^backup/(demo-(lxc|qemu)-(\d+)-(\d+))(\.tar)?$!) {
        my $name = $1 . ($5 // '');
        my $vmid = $3;
        my $fmt  = defined($5) ? 'tar' : 'raw';
        return ('backup', $name, $vmid, undef, undef, undef, $fmt);
    }

    return $class->SUPER::parse_volname($volname);
}

sub _list_demo_backups {
    my ($class, $storeid, $path, $vmid) = @_;

    my $res = [];

        foreach my $file (glob("$path/demo-lxc-*-*.tar")) {
            next if $vmid && $file !~ /demo-lxc-$vmid-/;
            my $st = File::stat::stat($file) or next;
            my $name = basename($file);
            push @$res, {
                volid   => "$storeid:backup/$name",
                format  => 'tar',
                size    => $st->size,
                ctime   => $st->ctime,
                content => 'backup',
                vmid    => ($name =~ /^demo-lxc-(\d+)-/ ? $1 : undef),
                subtype => 'lxc',
            };
        }

    foreach my $conf (glob("$path/demo-qemu-*-*.conf")) {
        next if $vmid && $conf !~ /demo-qemu-$vmid-/;
        my $prefix = $conf;
        $prefix =~ s/\.conf$//;
        my $name = basename($prefix);
        my $size = 0;
        my $ctime = 0;
        for my $f (glob("$prefix-*.raw"), $conf) {
            my $st = File::stat::stat($f) or next;
            $size += $st->size;
            $ctime = $st->ctime if $st->ctime > $ctime;
        }
        push @$res, {
            volid   => "$storeid:backup/$name",
            format  => 'raw',
            size    => $size,
            ctime   => $ctime,
            content => 'backup',
            vmid    => ($name =~ /^demo-qemu-(\d+)-/ ? $1 : undef),
            subtype => 'qemu',
        };
    }

    return $res;
}

sub list_volumes {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    my $res = [];
    my @other = ();
    for my $type (@$content_types) {
        if ($type eq 'backup') {
            my $dir = $class->get_subdir($scfg, 'backup');
            push @$res, @{ $class->_list_demo_backups($storeid, $dir, $vmid) };
        } else {
            push @other, $type;
        }
    }

    if (@other) {
        push @$res, @{ $class->SUPER::list_volumes($storeid, $scfg, $vmid, \@other) };
    }

    return $res;
}

sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} //= '/tmp/demo';

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

sub new_backup_provider {
    my ($class, $scfg, $storeid, $log_function) = @_;
    require PVE::BackupProvider::Plugin::Demo;
    return PVE::BackupProvider::Plugin::Demo->new($class, $scfg, $storeid, $log_function);
}

sub api {
    return PVE::Storage::APIVER;
}

1;
