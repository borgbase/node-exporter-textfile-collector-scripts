#!/usr/bin/env python3

import re
import sys
import argparse
import os.path
import subprocess
import configparser
import urllib.request
import urllib.parse
import socket
import logging
import logging.handlers


BINARIES = {
    'megacli': {
        'path': '/usr/local/sbin/megacli',
        'checks': [
            {
                'name': 'Enclosure state',
                'args': ['-EncInfo', '-aALL'],
                'test': 'megacli/enc.txt',
                'values': [
                    ['Status', 'Normal'],
                ]
            },
            {
                'name': 'Virtual drive state',
                'args': ['-LDInfo', '-Lall', '-aALL', '-NoLog'],
                'test': 'megacli/ld.txt',
                'values': [
                    ['State', 'Optimal'],
                    ['Bad Blocks Exist', 'No']
                ]
            },
            {
                'name': 'Physical drive state',
                'args': ['-PDList', '-aALL'],
                'test': 'megacli/pd.txt',
                'values': [
                    ['Media Error Count', '0'],
                    # ['Other Error Count', '0'],
                    ['Predictive Failure Count', '0'],
                    ['Firmware state', 'Online, Spun Up'],
                    ['Drive has flagged a S\.M\.A\.R\.T alert', 'No'],
                ]
            },
            {
                'name': 'Patrol read state',
                'args': ['-AdpPR', '-Info', '-aALL', '-NoLog'],
                'test': 'megacli/pr.txt',
                'values': [
                    ['Patrol Read Mode', 'Auto']
                ]
            },
            {
                'name': 'Battery state',
                'args': ['-AdpBbuCmd', '-aALL', '-NoLog'],
                'test': 'megacli/battery.txt',
                'values': [
                    ['Battery State', 'Optimal'],
                ]
            }
        ]
    },
    'arcconf': {
        'path': '/usr/local/sbin/arcconf',
        'checks': [
            {
                'name': 'Controller status',
                'args': ['GETCONFIG', '1', 'AD'],
                'test': 'arcconf/ad.txt',
                'values': [
                    # arcconf name, prom metric name, expected value
                    ['Controller Status', 'controller_has_error', 'Optimal'],
                    ['Logical devices/Failed/Degraded', 'logical_device_degraded', '1/0/0'],
                    ['Defunct disk drive count', 'has_defunct_drives', '0']
                ]
            },
            {
                'name': 'Logical device state',
                'args': ['GETCONFIG', '1', 'LD'],
                'test': 'arcconf/ld.txt',
                'values': [
                    ['Status of Logical Device', 'logical_device_has_error', 'Optimal'],
                    ['Parity Initialization Status', 'parity_initialization_incomplete', 'Completed'],
                ]
            },
            {
                'name': 'Physical device state',
                'args': ['GETCONFIG', '1', 'PD'],
                'test': 'arcconf/pd.txt',
                'separator': {
                    'regex': '\s+Device #(\d+)',
                    'label': 'device'
                },
                'values': [
                    ['State', 'drive_offline', 'Online'],
                    ['S.M.A.R.T. warnings', 'drive_has_smart_warnings', '0'],
                    # # ['Unused Size', '0 MB'],
                    ['Aborted Commands', 'drive_has_aborted_commands', '0'],
                    ['Bad Target Errors', 'drive_has_bad_target_errors', '0'],
                    # ['Ecc Recovered Read Errors', 0],
                    # ['Failed Read Recovers', 0],
                    # ['Failed Write Recovers', 0],
                    ['Format Errors', 'drive_has_format_errors', '0'],
                    ['Hardware Errors', 'drive_has_hardware_errors', '0'],
                    ['Hard Read Errors', 'drive_has_hw_read_errors', '0'],
                    ['Hard Write Errors', 'drive_has_hw_read_errors', '0'],
                    ['Media Failures', 'drive_has_media_failures', '0'],
                    ['Not Ready Errors', 'drive_has_not_ready_errors', '0'],
                    # # ['Other Time Out Errors', 0],
                    ['Predictive Failures', 'drive_has_predictive_failures', '0'],
                    # ['Retry Recovered Read Errors', 0],
                    # ['Retry Recovered Write Errors', 0],
                    ['Scsi Bus Faults', 'drive_has_scsi_bus_faults', '0']
                ]
            }
        ]
    }
}

# Parse cli args
parser = argparse.ArgumentParser(description='Find failed values in RAID management tools.')
parser.add_argument('-b', '--vendor-binary', default='arcconf',
                    help='which vendor binary to use. E.g. megacli, arcconf')
parser.add_argument('-c', '--config', default='/etc/raid-monitor.conf',
                    help='path to config file in INI format. Default: /etc/raid-monitor.conf')
parser.add_argument('--debug', action="store_true", default=False,
                    help='compare values from provided mock files')
args = parser.parse_args()


# Configure logging
format = f"%(asctime)s {socket.gethostname()} raid: %(message)s"
formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(console_handler)
logger.setLevel(logging.ERROR)
if args.debug:
    logger.setLevel(logging.DEBUG)


def collect_metrics():
    """
    Read output from vendor binary and compare significant lines
    to expected result.

    Return True if any result is unexpected.
    """
    metrics = []

    binary_name = args.vendor_binary or config.get('vendor', 'binary', fallback=False)
    binary_config = BINARIES.get(binary_name)  # TODO: read path from config, rename config stuff
    if binary_config is None:
        logger.error('Unknown binary or no binary specified.')
        sys.exit(1)

    if not args.debug and not os.path.isfile(binary_config['path']):
        logger.error('Supporting vendor binary not found.')
        sys.exit(1)

    for check in binary_config['checks']:
        logger.debug('Running check: %s', check['name'])
        separator_value = None
        separator_label = None
        
        if args.debug:
            cmd_res = open(os.path.join('mocks', check['test'])).read()
        else:
            proc_res = subprocess.run(
                [binary_config['path']] + check['args'],
                stdout=subprocess.PIPE, check=True, universal_newlines=True)
            cmd_res = proc_res.stdout
            if len(cmd_res) == 0 or proc_res.returncode != 0:
                logger.error('Vendor binary returned empty result or error.')
                sys.exit(1)

        for line in cmd_res.splitlines():
            # try to update separator
            separator_dict = check.get('separator')
            if separator_dict is not None:
                res = re.match(separator_dict['regex'], line)
                if res:
                    separator_value = res.group(1)
                    separator_label = separator_dict['label']


            for attribute_name, metric_name, expected_value in check['values']:
                res = re.match(f"^\s*{attribute_name}\s*:\s*(\w+.*)$", line)
                if res is not None:
                    metrics.append([
                        metric_name,
                        separator_label,
                        separator_value,
                        int(res.group(1) != expected_value),
                        res.group(1)
                    ])


    logger.info('Finished checks for vendor %s', binary_name)
    return metrics


def print_all_metrics(metrics):
    for metric, label_name, label_value, status, metric_value in metrics:
        if label_name is not None:
            print(f'{args.vendor_binary}_{metric}{{{label_name}="{label_value}",state="{metric_value}"}} {status}')
        else:
            print(f'{args.vendor_binary}_{metric}{{state="{metric_value}"}} {status}')

def main():
    metrics = collect_metrics()
    print_all_metrics(metrics)


if __name__ == '__main__':
    main()
