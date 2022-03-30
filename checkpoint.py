#!/usr/bin/python3

import logging
import json
import os

from absl import flags

flags.DEFINE_string("checkpoint_filename", "checkpoint.json",
                    "A checkpoint file which records progress so that the dump "
                    "can resume upon crashing.")

flags.DEFINE_boolean("resume_from_checkpoint", True,
                    "If set to true, resumes from the checkpoint, ignoring any "
                    "start and end dates that are passed on through command line "
                    "flags. This is useful if you'd like the a given invocation to "
                    "run to completion. If set to false, and if the start and end dates "
                    "are different from what is in the checkpoint, then the checkpoint is "
                    "ignored and overwritten.")

FLAGS = flags.FLAGS

CP_STARTDATE = 'startdate'
CP_ENDDATE = 'enddate'
CP_NEXTDATE = 'nextdate'


class Checkpoint(object):

    def __init__(self, startdate, enddate):
        self.startdate = startdate
        self.enddate = enddate

        start_str, end_str = startdate.isoformat()[0:10], enddate.isoformat()[0:10]
        try:

            self.cstate = json.loads(open(FLAGS.checkpoint_filename, "r").read())
            if self.cstate[CP_STARTDATE] == start_str and self.cstate[CP_ENDDATE] == end_str:
                logging.info("Found a checkpoint: %s", self.cstate)
                return
            else:
                logging.warning("Previous checkpoint was for a different time range from %s - %s",
                        start_str, end_str)
                if FLAGS.resume_from_checkpoint:
                    logging.warning("resume_from_checkpoint=True, using checkpoint start and end dates.");
                    logging.warning("If this is not intended, delete the checkpoint file.")
                    return
                self.delete_checkpoint()
        except FileNotFoundError:
            logging.info("No existing checkpoint.")
        except json.JSONDecodeError:
            logging.info("Corrupt checkpoint file, overwriting.")  
        self.cstate = {
                CP_STARTDATE: start_str,
                CP_ENDDATE: end_str,
                CP_NEXTDATE: start_str
            }

    def nextdate(self):
        return self.cstate[CP_NEXTDATE]

    def delete_checkpoint(self):
        logging.info("Deleting checkpoint.")
        os.unlink(FLAGS.checkpoint_filename)

    def write_checkpoint(self, nextdate):
        self.cstate[CP_NEXTDATE] = nextdate.isoformat()[0:10]
        logging.info("Writing %s to checkpoint.", self.cstate)
        open(FLAGS.checkpoint_filename, 'w').write(json.dumps(self.cstate))

