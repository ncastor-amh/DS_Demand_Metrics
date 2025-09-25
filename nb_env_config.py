# Databricks notebook source
ENV = dbutils.secrets.get(scope="dsvc-dbks-secret-scope", key="dsvc-env")

UNGOV_TABLE = f'dsvc_{ENV}_ungov'
GOV_FIN_TABLE = f'dsvc_{ENV}_gov_fin'
GOV_OTHERS_TABLE = f'dsvc_{ENV}_gov_others'
OUTREACH_SCHEMA = 'enr_outreach'